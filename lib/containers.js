/* jshint node: true */

// TODO: Add streams which prefix each record with its length.

'use strict';

/**
 * This module defines custom streams to write and read Avro files.
 *
 * In particular, the `Block{En,De}coder` streams are able to deal with Avro
 * container files. None of the streams below depend on the filesystem however,
 * this way they can also be used in the browser (for example to parse HTTP
 * responses).
 */

var types = require('./types'),
    utils = require('./utils'),
    stream = require('stream'),
    util = require('util'),
    zlib = require('zlib');


var OPTS = {namespace: 'org.apache.avro.file'};

var LONG_TYPE = types.Type.forSchema('long', OPTS);

var MAP_BYTES_TYPE = types.Type.forSchema({type: 'map', values: 'bytes'}, OPTS);

var HEADER_TYPE = types.Type.forSchema({
  name: 'Header',
  type: 'record',
  fields : [
    {name: 'magic', type: {type: 'fixed', name: 'Magic', size: 4}},
    {name: 'meta', type: MAP_BYTES_TYPE},
    {name: 'sync', type: {type: 'fixed', name: 'Sync', size: 16}}
  ]
}, OPTS);

var BLOCK_TYPE = types.Type.forSchema({
  name: 'Block',
  type: 'record',
  fields : [
    {name: 'count', type: 'long'},
    {name: 'data', type: 'bytes'},
    {name: 'sync', type: 'Sync'}
  ]
}, OPTS);

// First 4 bytes of an Avro object container file.
var MAGIC_BYTES = utils.bufferFrom('Obj\x01');

// Default bounds on the number and total size (in bytes) of blocks which can
// be held at once. These apply to both blocks being processed (in-flight) and
// blocks which finished processing but are waiting to be delivered in order,
// so that a slow consumer cannot cause unbounded buffering.
var DEFAULT_MAX_BLOCKS = 100;
var DEFAULT_MAX_BYTES = 10 * 1024 * 1024;

// Convenience.
var f = util.format;
var Tap = utils.Tap;


/** Duplex stream for decoding fragments. */
function RawDecoder(schema, opts) {
  opts = opts || {};

  var noDecode = !!opts.noDecode;
  stream.Duplex.call(this, {
    readableObjectMode: !noDecode,
    allowHalfOpen: false
  });

  this._type = types.Type.forSchema(schema);
  this._tap = new Tap(utils.newBuffer(0));
  this._writeCb = null;
  this._needPush = false;
  this._readValue = createReader(noDecode, this._type);
  this._finished = false;

  this.on('finish', function () {
    this._finished = true;
    this._read();
  });
}
util.inherits(RawDecoder, stream.Duplex);

RawDecoder.prototype._write = function (chunk, encoding, cb) {
  // Store the write callback and call it when we are done decoding all records
  // in this chunk. If we call it right away, we risk loading the entire input
  // in memory. We only need to store the latest callback since the stream API
  // guarantees that `_write` won't be called again until we call the previous.
  this._writeCb = cb;

  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;
  if (this._needPush) {
    this._needPush = false;
    this._read();
  }
};

RawDecoder.prototype._read = function () {
  this._needPush = false;

  var tap = this._tap;
  var pos = tap.pos;
  var val = this._readValue(tap);
  if (tap.isValid()) {
    this.push(val);
  } else if (!this._finished) {
    tap.pos = pos;
    this._needPush = true;
    if (this._writeCb) {
      // This should only ever be false on the first read, and only if it
      // happens before the first write.
      this._writeCb();
    }
  } else {
    this.push(null);
  }
};


/** Duplex stream for decoding object container files. */
function BlockDecoder(opts) {
  opts = opts || {};

  var noDecode = !!opts.noDecode;
  stream.Duplex.call(this, {
    allowHalfOpen: true, // For async decompressors.
    readableObjectMode: !noDecode
  });

  this._rType = opts.readerSchema !== undefined ?
    types.Type.forSchema(opts.readerSchema) :
    undefined;
  this._wType = null;
  this._codecs = opts.codecs;
  this._codec = undefined;
  this._parseHook = opts.parseHook;
  this._tap = new Tap(utils.newBuffer(0));
  this._blockTap = new Tap(utils.newBuffer(0));
  this._syncMarker = null;
  this._readValue = null;
  this._noDecode = noDecode;
  this._dispatcher = null; // Lazily created once the header is decoded.
  this._remaining = undefined; // In the current block.
  this._needPush = false;
  this._readPending = false;
  this._finished = false;
  this._maxBlocks = getLimit(opts.maxBlocks, DEFAULT_MAX_BLOCKS);
  this._maxBytes = getLimit(opts.maxBlockBytes, DEFAULT_MAX_BYTES);
  trackBackpressure(this);

  this.on('finish', function () {
    this._finished = true;
    // Pump again on the next tick: this terminates the stream if everything
    // was delivered, or arms `_needPush` so the last in-flight decompression
    // ends it. The deferral keeps the push out of the writable call stack.
    this._needPush = false;
    if (this._dispatcher) {
      var self = this;
      process.nextTick(function () { self._pump(); });
    }
  });
}
util.inherits(BlockDecoder, stream.Duplex);

BlockDecoder.defaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.inflateRaw
  };
};

BlockDecoder.getDefaultCodecs = BlockDecoder.defaultCodecs;

BlockDecoder.prototype._decodeHeader = function () {
  var tap = this._tap;
  if (tap.buf.length < MAGIC_BYTES.length) {
    // Wait until more data arrives.
    return false;
  }

  if (!MAGIC_BYTES.equals(tap.buf.slice(0, MAGIC_BYTES.length))) {
    this.emit('error', new Error('invalid magic bytes'));
    return false;
  }

  var header = HEADER_TYPE._read(tap);
  if (!tap.isValid()) {
    return false;
  }

  this._codec = (header.meta['avro.codec'] || 'null').toString();
  var codecs = this._codecs || BlockDecoder.getDefaultCodecs();
  var decompress = codecs[this._codec];
  if (!decompress) {
    this.emit('error', new Error(f('unknown codec: %s', this._codec)));
    return;
  }

  try {
    var schema = JSON.parse(header.meta['avro.schema'].toString());
    if (this._parseHook) {
      schema = this._parseHook(schema);
    }
    this._wType = types.Type.forSchema(schema);
  } catch (err) {
    this.emit('error', err);
    return;
  }

  try {
    this._readValue = createReader(this._noDecode, this._wType, this._rType);
  } catch (err) {
    this.emit('error', err);
    return;
  }

  var self = this;
  this._dispatcher = new BlockDispatcher(decompress, {
    codec: this._codec,
    maxBlocks: this._maxBlocks,
    maxBytes: this._maxBytes,
    onBlock: function (state) {
      self.emit('block', new BlockInfo(
        state.count, state.buf.length, state.size
      ));
    },
    onReady: function () {
      // A block finished decompressing: deliver it and any blocks queued
      // behind it, preserving order. Deferred out of the codec callback's
      // call stack; when the consumer is stalled the backpressure latch keeps
      // `_needPush` set and `resume` drives the pump instead.
      if (!self._backpressure) {
        self._needPush = false;
        process.nextTick(function () { self._pump(); });
      }
    },
    onError: function (cause) {
      var err = new Error(f('%s codec decompression error', self._codec));
      err.cause = cause;
      self.emit('error', err);
    }
  });

  this._syncMarker = header.sync;
  this.emit('metadata', this._wType, this._codec, header);
  return true;
};

BlockDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf, chunk]);
  tap.pos = 0;

  if (!this._decodeHeader()) {
    process.nextTick(cb);
    return;
  }

  // We got the header, switch to block decoding mode. Also, call it directly
  // in case we already have all the data (in which case `_write` wouldn't get
  // called anymore).
  this._write = this._writeChunk;
  this._write(utils.newBuffer(0), encoding, cb);
};

BlockDecoder.prototype._writeChunk = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf.slice(tap.pos), chunk]);
  tap.pos = 0;

  var dispatcher = this._dispatcher;
  var block;
  while ((block = tryReadBlock(tap))) {
    if (!this._syncMarker.equals(block.sync)) {
      this.emit('error', new Error('invalid sync marker'));
      return;
    }
    dispatcher.submit({
      buf: block.data,
      size: block.data.length,
      count: block.count
    });
  }
  // The write is complete once all blocks from this chunk have been delivered
  // (not merely scheduled): this provides backpressure on the writable side
  // whenever the bounded queues are full or a slow consumer stalls delivery.
  dispatcher.wait(cb);
};

BlockDecoder.prototype._read = function () {
  this._pump();
};

/**
 * Decode and deliver records from blocks ready in the scheduler.
 *
 * This is the only place data is pushed. It may run from a read cycle, a codec
 * completion, a `resume`, or a deferred retry. It pushes records while the
 * consumer accepts data; on backpressure it stops and the `resume` handler
 * pumps again. When no block is ready, a single timer-based retry keeps the
 * read cycle alive (the deferred push clears the reading flag) without pushing
 * empty objects or busy-looping, so a slow decompressor cannot deadlock a pipe.
 */
BlockDecoder.prototype._pump = function () {
  if (this.destroyed || this.readableEnded) {
    return;
  }

  var dispatcher = this._dispatcher;
  var tap = this._blockTap;
  var delivered = false;

  while (true) {
    // Cross object/byte-mode backpressure; `resume` continues the pump.
    if (this.readableFlowing === false) {
      this._needPush = true;
      return;
    }
    if (!this._remaining) {
      // No record left in the current block, grab the next delivered one.
      var state = dispatcher && dispatcher.pop();
      if (!state) {
        if (this._finished && (!dispatcher || dispatcher.isDrained())) {
          this.push(null);
        } else {
          // The next block is still being decompressed.
          this._needPush = true;
          if (!delivered) {
            this._scheduleRead();
          }
        }
        return;
      }
      this._remaining = state.count;
      tap.buf = state.buf;
      tap.pos = 0;
      if (!this._remaining) {
        continue; // Empty block, fetch the next one.
      }
    }

    this._remaining--;
    var val;
    try {
      val = this._readValue(tap);
      if (!tap.isValid()) {
        throw new Error('truncated block');
      }
    } catch (err) {
      this._remaining = 0;
      this.emit('error', err); // Corrupt data.
      return;
    }
    if (this.push(val) === false) {
      // Readable buffer full: the `resume` handler pumps once it drains.
      this._needPush = true;
      return;
    }
    delivered = true;
  }
};

/** Schedule at most one deferred pump to keep a pending read cycle alive. */
BlockDecoder.prototype._scheduleRead = function () {
  if (this._readPending) {
    return;
  }
  this._readPending = true;
  var self = this;
  // A timer yields the event loop so slow decompression callbacks can run.
  var timer = setTimeout(function () {
    self._readPending = false;
    if (!self.destroyed && !self.readableEnded) {
      self._pump();
    }
  }, 0);
  if (typeof timer.unref === 'function') {
    timer.unref(); // Don't keep the process alive once real work is done.
  }
};

BlockDecoder.prototype._destroy = function (err, cb) {
  if (this._dispatcher) {
    this._dispatcher.destroy();
  }
  cb(err);
};


/** Duplex stream for encoding. */
function RawEncoder(schema, opts) {
  opts = opts || {};

  stream.Transform.call(this, {
    writableObjectMode: true,
    allowHalfOpen: false
  });

  this._type = types.Type.forSchema(schema);
  this._writeValue = function (tap, val) {
    try {
      this._type._write(tap, val);
    } catch (err) {
      this.emit('typeError', err, val, this._type);
    }
  };
  this._tap = new Tap(utils.newBuffer(opts.batchSize || 65536));

  this.on('typeError', function (err) { this.emit('error', err); });
}
util.inherits(RawEncoder, stream.Transform);

RawEncoder.prototype._transform = function (val, encoding, cb) {
  var tap = this._tap;
  var buf = tap.buf;
  var pos = tap.pos;

  this._writeValue(tap, val);
  if (!tap.isValid()) {
    if (pos) {
      // Emit any valid data.
      this.push(copyBuffer(tap.buf, 0, pos));
    }
    var len = tap.pos - pos;
    if (len > buf.length) {
      // Not enough space for last written object, need to resize.
      tap.buf = utils.newBuffer(2 * len);
    }
    tap.pos = 0;
    this._writeValue(tap, val); // Rewrite last failed write.
  }

  cb();
};

RawEncoder.prototype._flush = function (cb) {
  var tap = this._tap;
  var pos = tap.pos;
  if (pos) {
    // This should only ever be false if nothing is written to the stream.
    this.push(tap.buf.slice(0, pos));
  }
  cb();
};


/**
 * Duplex stream to write object container files.
 *
 * @param schema
 * @param opts {Object}
 *
 *  + `blockSize`, uncompressed.
 *  + `codec`
 *  + `codecs`
 *  + `maxBlockBytes`, maximum total bytes buffered (in-flight or waiting).
 *  + `maxBlocks`, maximum number of blocks buffered (in-flight or waiting).
 *  + `metadata``
 *  + `noCheck`
 *  + `omitHeader`, useful to append to an existing block file.
 */
function BlockEncoder(schema, opts) {
  opts = opts || {};

  stream.Duplex.call(this, {
    allowHalfOpen: true, // To support async compressors.
    writableObjectMode: true
  });

  var type;
  if (types.Type.isType(schema)) {
    type = schema;
    schema = undefined;
  } else {
    // Keep full schema to be able to write it to the header later.
    type = types.Type.forSchema(schema);
  }

  this._schema = schema;
  this._type = type;
  this._writeValue = function (tap, val) {
    try {
      this._type._write(tap, val);
    } catch (err) {
      this.emit('typeError', err, val, this._type);
      return false;
    }
    return true;
  };
  this._blockSize = opts.blockSize || 65536;
  this._tap = new Tap(utils.newBuffer(this._blockSize));
  this._codecs = opts.codecs;
  this._codec = opts.codec || 'null';
  this._blockCount = 0;
  this._syncMarker = opts.syncMarker || new utils.Lcg().nextBuffer(16);
  this._finished = false;
  this._needHeader = false;
  this._needPush = false;
  this._readPending = false;

  this._metadata = opts.metadata || {};
  if (!MAP_BYTES_TYPE.isValid(this._metadata)) {
    throw new Error('invalid metadata');
  }

  var codec = this._codec;
  var compress = (this._codecs || BlockEncoder.getDefaultCodecs())[codec];
  if (!compress) {
    throw new Error(f('unsupported codec: %s', codec));
  }

  var self = this;
  this._dispatcher = new BlockDispatcher(compress, {
    codec: codec,
    maxBlocks: getLimit(opts.maxBlocks, DEFAULT_MAX_BLOCKS),
    maxBytes: getLimit(opts.maxBlockBytes, DEFAULT_MAX_BYTES),
    onBlock: function (state) {
      self.emit('block', new BlockInfo(
        state.count, state.size, state.buf.length
      ));
    },
    onReady: function () {
      // A block finished processing: deliver it (and any earlier blocks that
      // were queued behind it). The pump is deferred out of the codec
      // callback's call stack so it never runs while the readable stream's
      // internal reading flag is set (which happens for synchronous codecs
      // invoked inside a writable `_write`). When the consumer is stalled the
      // backpressure latch keeps `_needPush` set and `resume` pumps instead.
      if (!self._backpressure) {
        self._needPush = false;
        process.nextTick(function () { self._pump(); });
      }
    },
    onError: function (cause) {
      var err = new Error(f('%s codec compression error', self._codec));
      err.cause = cause;
      self.emit('error', err);
    }
  });

  if (opts.omitHeader !== undefined) { // Legacy option.
    opts.writeHeader = opts.omitHeader ? 'never' : 'auto';
  }
  switch (opts.writeHeader) {
    case false:
    case 'never':
      break;
    case undefined: // Backwards-compatibility (eager default would be better).
    case 'auto':
      this._needHeader = true;
      break;
    default:
      this._writeHeader();
  }

  this.on('finish', function () {
    this._finished = true;
    if (this._blockCount) {
      this._flushChunk();
    }
    // Pump again on the next tick: either this terminates the stream (if
    // nothing remains), or it arms `_needPush` so the last in-flight block's
    // completion ends it. The deferral keeps the push out of the writable
    // call stack to avoid a stuck reading flag under backpressure.
    this._needPush = false;
    var self = this;
    process.nextTick(function () { self._pump(); });
  });

  this.on('typeError', function (err) { this.emit('error', err); });

  trackBackpressure(this);
}
util.inherits(BlockEncoder, stream.Duplex);

BlockEncoder.defaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.deflateRaw
  };
};

BlockEncoder.getDefaultCodecs = BlockEncoder.defaultCodecs;

BlockEncoder.prototype._writeHeader = function () {
  var schema = JSON.stringify(
    this._schema ? this._schema : this._type.getSchema({exportAttrs: true})
  );
  var meta = utils.copyOwnProperties(
    this._metadata,
    {'avro.schema': utils.bufferFrom(schema), 'avro.codec': utils.bufferFrom(this._codec)},
    true // Overwrite.
  );
  var Header = HEADER_TYPE.getRecordConstructor();
  var header = new Header(MAGIC_BYTES, meta, this._syncMarker);
  this.push(header.toBuffer());
};

BlockEncoder.prototype._write = function (val, encoding, cb) {
  if (this._needHeader) {
    this._writeHeader();
    this._needHeader = false;
  }

  var tap = this._tap;
  var pos = tap.pos;
  var flushing = false;

  if (this._writeValue(tap, val)) {
    if (!tap.isValid()) {
      if (pos) {
        this._flushChunk(pos, cb);
        flushing = true;
      }
      var len = tap.pos - pos;
      if (len > this._blockSize) {
        // Not enough space for last written object, need to resize.
        this._blockSize = len * 2;
      }
      tap.buf = utils.newBuffer(this._blockSize);
      tap.pos = 0;
      this._writeValue(tap, val); // Rewrite last failed write.
    }
    this._blockCount++;
  } else {
    tap.pos = pos;
  }

  if (!flushing) {
    cb();
  }
};

BlockEncoder.prototype._flushChunk = function (pos, cb) {
  var tap = this._tap;
  pos = pos || tap.pos;
  var count = this._blockCount;
  this._blockCount = 0;
  // The write callback (when any) is released when the compressed block is
  // delivered in order, bounding the number of blocks held by the scheduler.
  this._dispatcher.submit({
    buf: tap.buf.slice(0, pos),
    size: pos,
    count: count,
    cb: cb
  });
};

BlockEncoder.prototype._read = function () {
  this._pump();
};

/**
 * Deliver any blocks ready in the scheduler.
 *
 * This is the only place data is pushed to the readable side. It may run from
 * a read cycle, a codec completion, a `resume`, or a deferred retry. It pushes
 * every available block while the consumer accepts data. Because this is a
 * byte stream potentially piped into an object-mode destination, the consumer
 * can pause while this stream's own buffer still has room (so `push` keeps
 * returning true): the live `readableFlowing` state detects that, and the
 * `resume` handler re-invokes the pump on the next drain.
 *
 * When no block is ready yet, a single timer-based retry keeps the read cycle
 * alive. No empty buffer is ever pushed: empty chunks stall object-mode
 * destinations. Termination (`push(null)`) is unconditional once the scheduler
 * is fully drained after `finish`, rather than gated on the buffer being empty.
 */
BlockEncoder.prototype._pump = function () {
  if (this.destroyed || this.readableEnded) {
    return;
  }

  var dispatcher = this._dispatcher;
  var delivered = false;
  while (true) {
    // Cross object/byte-mode backpressure; the `resume` handler continues.
    if (this.readableFlowing === false) {
      this._needPush = true;
      return;
    }
    var state = dispatcher.pop();
    if (!state) {
      break;
    }
    if (this.push(LONG_TYPE.toBuffer(state.count, true)) === false ||
        this.push(LONG_TYPE.toBuffer(state.buf.length, true)) === false ||
        this.push(state.buf) === false ||
        this.push(this._syncMarker) === false) {
      this._needPush = true;
      return;
    }
    delivered = true;
  }

  if (this._finished && dispatcher.isDrained()) {
    // Unconditional end: gating on an empty buffer can leave the stream's
    // reading flag stuck when the last chunk is still queued for the sink.
    this.push(null);
    return;
  }
  // Nothing ready right now; the next compression completion pumps. When this
  // pump delivered nothing, schedule a deferred retry so a pending read cycle
  // is kept alive without busy-looping or pushing an empty chunk.
  this._needPush = true;
  if (!delivered) {
    this._scheduleRead();
  }
};

/**
 * Schedule at most one deferred pump to keep a pending read cycle alive.
 *
 * A timer is used rather than `nextTick`: a `nextTick` chain waiting on a slow
 * codec callback would keep the microtask queue non-empty and starve the timer
 * phase, deadlocking that callback. The timer is `unref`'d so it never keeps
 * the process alive after all real work is done. The guard keeps at most one
 * retry in flight.
 */
BlockEncoder.prototype._scheduleRead = function () {
  if (this._readPending) {
    return;
  }
  this._readPending = true;
  var self = this;
  var timer = setTimeout(function () {
    self._readPending = false;
    if (!self.destroyed && !self.readableEnded) {
      self._pump();
    }
  }, 0);
  if (typeof timer.unref === 'function') {
    timer.unref(); // Don't keep the process alive once real work is done.
  }
};

BlockEncoder.prototype._destroy = function (err, cb) {
  this._dispatcher.destroy();
  cb(err);
};


/**
 * Bounded scheduler for asynchronous block processing.
 *
 * Blocks are processed concurrently (a codec may finish a later block before
 * an earlier one) but delivered in their original submission order. At any
 * time the number of blocks and their combined size, across both blocks being
 * processed and blocks waiting to be delivered, are kept below configurable
 * limits. This means that a slow downstream consumer cannot make the queue of
 * completed blocks grow without bound: further processing (and therefore
 * further writes) is paused until delivery catches up.
 *
 * Each block's terminal action (write callback, wait callback, error) is run
 * at most once; duplicate codec callbacks and calls after the dispatcher has
 * been destroyed are ignored.
 */
function BlockDispatcher(codec, opts) {
  opts = opts || {};

  this._codec = codec;
  this._maxBlocks = getLimit(opts.maxBlocks, DEFAULT_MAX_BLOCKS);
  this._maxBytes = getLimit(opts.maxBytes, DEFAULT_MAX_BYTES);
  this._onBlock = opts.onBlock || function () {};
  this._onReady = opts.onReady || function () {};
  this._onError = opts.onError || function () {};

  this._index = 0; // Next submission index.
  this._nextIndex = 0; // Index of the next block to deliver.
  this._waiting = []; // Submitted blocks not yet processed (input queue).
  this._inFlight = {}; // Index -> state, currently being processed.
  this._inFlightCount = 0;
  this._done = []; // Processed blocks awaiting in-order delivery.
  this._bytes = 0; // Total bytes held (waiting, in-flight and done).
  this._waiters = []; // Callbacks waiting for all submitted blocks to drain.
  this._destroyed = false;
}

BlockDispatcher.prototype.submit = function (state) {
  if (this._destroyed) {
    // The owning stream is gone; release any write callback instead of
    // processing the block.
    if (state.cb) {
      state.cb();
    }
    return;
  }
  state.index = this._index++;
  state.done = false;
  state.ended = false;
  this._waiting.push(state);
  this._bytes += state.size;
  this._dispatch();
};

BlockDispatcher.prototype.pop = function () {
  var state = this._done.length && this._done[0];
  if (!state || state.index !== this._nextIndex) {
    return null; // The next block in order hasn't been processed yet.
  }
  this._done.shift();
  this._nextIndex++;
  this._bytes -= state.buf.length;
  if (state.cb && !state.ended) {
    state.ended = true;
    state.cb();
  }
  // Freeing a slot may allow queued blocks to start processing.
  this._dispatch();
  this._releaseWaitersIfDrained();
  return state;
};

/** Run `cb` once every block submitted so far has been delivered. */
BlockDispatcher.prototype.wait = function (cb) {
  if (this._destroyed || this.isDrained()) {
    cb();
    return;
  }
  this._waiters.push(cb);
};

BlockDispatcher.prototype.isDrained = function () {
  return !this._waiting.length && !this._inFlightCount && !this._done.length;
};

BlockDispatcher.prototype.destroy = function () {
  this._destroyed = true;
  // Release any blocked write/wait callbacks exactly once and drop all held
  // blocks so the stream can be garbage collected.
  var state, i, l, key;
  for (i = 0, l = this._waiting.length; i < l; i++) {
    state = this._waiting[i];
    if (state.cb && !state.ended) {
      state.ended = true;
      state.cb();
    }
  }
  for (key in this._inFlight) {
    state = this._inFlight[key];
    if (state.cb && !state.ended) {
      state.ended = true;
      state.cb();
    }
  }
  for (i = 0, l = this._done.length; i < l; i++) {
    state = this._done[i];
    if (state.cb && !state.ended) {
      state.ended = true;
      state.cb();
    }
  }
  for (i = 0, l = this._waiters.length; i < l; i++) {
    this._waiters[i]();
  }
  this._waiting = [];
  this._inFlight = {};
  this._inFlightCount = 0;
  this._done = [];
  this._waiters = [];
  this._bytes = 0;
};

BlockDispatcher.prototype._dispatch = function () {
  while (this._waiting.length && this._hasCapacity()) {
    var state = this._waiting.shift();
    this._inFlight[state.index] = state;
    this._inFlightCount++;
    this._run(state);
  }
};

BlockDispatcher.prototype._hasCapacity = function () {
  var held = this._inFlightCount + this._done.length;
  if (held >= this._maxBlocks) {
    return false;
  }
  // A single block larger than the byte limit must still be able to make
  // progress: allow it through as long as no other block is currently held,
  // otherwise the scheduler would deadlock waiting for capacity it freed.
  return held === 0 || this._bytes < this._maxBytes;
};

BlockDispatcher.prototype._run = function (state) {
  var self = this;
  var called = false;
  try {
    this._codec(state.buf, function (cause, buf) {
      if (called || self._destroyed) {
        // Duplicate callback, or the owning stream was destroyed: ignore.
        return;
      }
      called = true;
      self._onComplete(state, cause, buf);
    });
  } catch (err) {
    if (called) {
      return;
    }
    called = true;
    this._onComplete(state, err);
  }
};

BlockDispatcher.prototype._onComplete = function (state, cause, buf) {
  if (this._destroyed) {
    return;
  }
  delete this._inFlight[state.index];
  this._inFlightCount--;

  if (cause) {
    this._fail(state, cause);
    return;
  }

  state.buf = buf;
  state.done = true;
  // In-flight bytes were accounted for at submission using the input size;
  // replace them with the output size, which is what remains held until
  // delivery.
  this._bytes += buf.length - state.size;
  this._insert(state);
  this._onBlock(state);
  this._onReady();
  this._releaseWaitersIfDrained();
  this._dispatch();
};

BlockDispatcher.prototype._fail = function (state, cause) {
  // The failed block will never be delivered; release its resources and any
  // terminal callback exactly once.
  this._bytes -= state.size;
  if (state.cb && !state.ended) {
    state.ended = true;
    state.cb();
  }
  this._releaseWaitersIfDrained();
  this._onError(cause);
};

/** Release drain waiters once nothing remains held by the scheduler. */
BlockDispatcher.prototype._releaseWaitersIfDrained = function () {
  if (this._waiters.length && this.isDrained()) {
    var waiters = this._waiters;
    this._waiters = [];
    waiters.forEach(function (cb) { cb(); });
  }
};

BlockDispatcher.prototype._insert = function (state) {
  // Insertion sort by index: `_done` is nearly sorted since blocks complete
  // roughly in order and only occasional reorderings occur.
  var done = this._done;
  var i = done.length;
  while (i > 0 && done[i - 1].index > state.index) {
    done[i] = done[i - 1];
    i--;
  }
  done[i] = state;
};


// Helpers.

/** Coerce a user-provided limit, ignoring invalid (non-positive) values. */
function getLimit(value, fallback) {
  value = +value;
  return value > 0 ? value : fallback;
}

/**
 * Latch downstream backpressure on a duplex stream.
 *
 * A block stream is byte-mode on its readable side while it may feed an
 * object-mode destination, so its own buffer can keep accepting pushes (and
 * `push` keeps returning true) long after the consumer has stalled. The
 * stream's `pause`/`resume` events, however, always reflect the real state of
 * the pipeline. We record that state in `_backpressure` and re-run the pump on
 * resume whenever more data was waiting to be delivered.
 */
function trackBackpressure(stream) {
  stream._backpressure = false;
  stream.on('pause', function () {
    // `pause` with a null flowing state happens during initial setup, before
    // any consumer is attached; only an actual transition to the paused
    // (false) state represents real downstream backpressure.
    if (stream.readableFlowing === false) {
      stream._backpressure = true;
    }
  });
  stream.on('resume', function () {
    stream._backpressure = false;
    // A drain may unblock delivery whether we were waiting on the consumer
    // (backpressure) or on a codec completion; re-evaluate in both cases.
    stream._needPush = false;
    stream._pump();
  });
}

/** Summary information about a block. */
function BlockInfo(count, raw, compressed) {
  this.valueCount = count;
  this.rawDataLength = raw;
  this.compressedDataLength = compressed;
}

/** Maybe get a block. */
function tryReadBlock(tap) {
  var pos = tap.pos;
  var block = BLOCK_TYPE._read(tap);
  if (!tap.isValid()) {
    tap.pos = pos;
    return null;
  }
  return block;
}

/** Create bytes consumer, either reading or skipping records. */
function createReader(noDecode, writerType, readerType) {
  if (noDecode) {
    return (function (skipper) {
      return function (tap) {
        var pos = tap.pos;
        skipper(tap);
        return tap.buf.slice(pos, tap.pos);
      };
    })(writerType._skip);
  } else if (readerType) {
    var resolver = readerType.createResolver(writerType);
    return function (tap) { return resolver._read(tap); };
  } else {
    return function (tap) { return writerType._read(tap); };
  }
}

/** Copy a buffer. This avoids creating a slice of the original buffer. */
function copyBuffer(buf, pos, len) {
  var copy = utils.newBuffer(len);
  buf.copy(copy, 0, pos, pos + len);
  return copy;
}


module.exports = {
  BLOCK_TYPE: BLOCK_TYPE, // For tests.
  HEADER_TYPE: HEADER_TYPE, // Idem.
  MAGIC_BYTES: MAGIC_BYTES, // Idem.
  streams: {
    BlockDecoder: BlockDecoder,
    BlockEncoder: BlockEncoder,
    RawDecoder: RawDecoder,
    RawEncoder: RawEncoder
  }
};
