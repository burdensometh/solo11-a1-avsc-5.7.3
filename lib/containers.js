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
  var position = opts.position;
  if (position === undefined) {
    position = opts.checkpoint;
  }
  if (position === undefined) {
    position = opts.resume;
  }
  if (position !== undefined && position !== null) {
    position = normalizePosition(position);
  } else {
    position = undefined;
  }

  stream.Duplex.call(this, {
    allowHalfOpen: true, // For async decompressors.
    readableObjectMode: !noDecode
  });

  this._rType = opts.readerSchema !== undefined ?
    types.Type.forSchema(opts.readerSchema) :
    undefined;
  this._wType = null;
  this._codec = undefined;
  this._codecs = opts.codecs;
  this._parseHook = opts.parseHook;
  this._fileId = null; // Identifies the file header (sync, schema, codec).
  this._schemaId = null; // Identifies the writer schema.
  this._tap = new Tap(utils.newBuffer(0));
  this._blockTap = new Tap(utils.newBuffer(0));
  this._syncMarker = null;
  this._readValue = null;
  this._noDecode = noDecode;
  this._queue = new utils.OrderedQueue();
  this._decompress = null; // Decompression function.
  this._index = 0; // Next block index in the internal queue.
  this._remaining = undefined; // Records left in the current block.
  this._needPush = false;
  this._finished = false;

  // Record-level positions.
  //
  // `_position` is the last acknowledged position, that is the position of the
  // last record which was actually handed over to the consumer. It never moves
  // ahead because of readable stream buffering or asynchronous decompression:
  // a record is acknowledged only once it leaves the internal readable buffer.
  //
  // In object mode, the buffer holds one object per record so the number of
  // delivered records is simply the number of pushed records minus the number
  // of buffered objects. In byte mode (`noDecode`), records can be concatenated
  // by `read`, so we remember each pushed record's end offset and count those
  // which have left the buffer.
  this._objectMode = !noDecode;
  this._blocks = []; // Metadata of all parsed blocks, in file order.
  this._pushed = 0; // Records pushed since tracking started.
  this._acked = position && position.record >= 0 ? position.record + 1 : 0;
  this._pushedBytes = 0; // Bytes pushed since tracking started (byte mode).
  this._recordEnds = []; // Cumulative end offsets of pushed records.
  this._boundary = 0; // Index of the first unacknowledged record end.
  this._position = position || null;
  this._positionRecord = position ? position.record : -1;
  this._recordsBefore = 0; // Records in all blocks before the current one.
  this._blockOrdinal = 0; // Number of blocks parsed so far.
  this._tapBase = 0; // Absolute file offset of the first byte of `_tap.buf`.
  this._resume = position || null;
  this._resumeDone = !position;
  this._mode = null; // One of `blocks`, `error`, or `header` (implicit).

  if (position && position.record >= 0) {
    // Records up to (and including) the checkpoint are already acknowledged.
    this._pushed = position.record + 1;
  }

  this.on('finish', function () {
    if (this._mode === 'error') {
      return;
    }
    if (!this._resumeDone) {
      // The block containing the checkpoint was never reached.
      this._emitError(new Error('checkpoint mismatch'));
      return;
    }
    this._finished = true;
    if (this._needPush) {
      this._read();
    }
  });
}
util.inherits(BlockDecoder, stream.Duplex);

/**
 * Number of records handed over to the consumer.
 *
 * This is derived from the readable buffer's state rather than from hooks on
 * `read` and `emit`: Node can bypass `read` (when flowing) and can coalesce
 * multiple pushed buffers into a single `read` in byte mode, so hooking those
 * methods is not sufficient.
 */
BlockDecoder.prototype._getDelivered = function () {
  var state = this._readableState;
  if (this._objectMode) {
    var buffered = state ? state.length : 0;
    return this._pushed - buffered;
  }

  // Byte mode: a read may return several records at once or part of one. A
  // record counts as delivered once all its bytes have left the buffer.
  var deliveredBytes = this._pushedBytes - (state ? state.length : 0);
  var ends = this._recordEnds;
  var boundary = this._boundary;
  while (boundary < ends.length && ends[boundary] <= deliveredBytes) {
    boundary++;
    this._acked++;
  }
  if (boundary > 1024) {
    // Drop already acknowledged record boundaries to bound memory usage.
    this._recordEnds = ends.slice(boundary);
    boundary = 0;
  }
  this._boundary = boundary;
  return this._acked;
};

/**
 * Get the position of the last record handed over to the consumer.
 *
 * The returned value is `null` before any record has been delivered (unless a
 * position was provided to resume from, in which case it is returned as is).
 */
BlockDecoder.prototype.getPosition = function () {
  var record = this._getDelivered() - 1;
  if (record < 0) {
    return this._position && this._position.record < 0 ? this._position : null;
  }
  if (record === this._positionRecord) {
    return this._position;
  }
  var block = this._blockAt(record);
  if (!block) {
    return this._position;
  }
  this._positionRecord = record;
  this._position = createPosition(
    this._fileId,
    this._schemaId,
    block.index,
    record - block.firstRecord,
    record,
    block.offset,
    block.sync,
    block.compressed,
    block.checksum
  );
  return this._position;
};

/** Alias for `getPosition`. */
BlockDecoder.prototype.getCheckpoint = BlockDecoder.prototype.getPosition;

Object.defineProperty(BlockDecoder.prototype, 'position', {
  get: function () { return this.getPosition(); }
});

Object.defineProperty(BlockDecoder.prototype, 'checkpoint', {
  get: function () { return this.getPosition(); }
});

/** Find the block containing a given record. */
BlockDecoder.prototype._blockAt = function (record) {
  var blocks = this._blocks;
  var lo = 0;
  var hi = blocks.length - 1;
  while (lo <= hi) {
    var mid = (lo + hi) >> 1;
    var block = blocks[mid];
    if (record < block.firstRecord) {
      hi = mid - 1;
    } else if (record >= block.firstRecord + block.count) {
      lo = mid + 1;
    } else {
      return block;
    }
  }
  return null;
};

BlockDecoder.prototype._emitError = function (err) {
  if (this._mode === 'error') {
    return; // Only emit the first error.
  }
  this._mode = 'error';
  this.emit('error', err);
};

BlockDecoder.defaultCodecs = function () {
  return {
    'null': function (buf, cb) { cb(null, buf); },
    'deflate': zlib.inflateRaw
  };
};

BlockDecoder.getDefaultCodecs = BlockDecoder.defaultCodecs;

BlockDecoder.prototype._decodeHeader = function () {
  var tap = this._tap;
  if (this._mode === 'error') {
    return false;
  }
  if (tap.buf.length < MAGIC_BYTES.length) {
    // Wait until more data arrives.
    return false;
  }

  if (!MAGIC_BYTES.equals(tap.buf.slice(0, MAGIC_BYTES.length))) {
    this._emitError(new Error('invalid magic bytes'));
    return false;
  }

  var header = HEADER_TYPE._read(tap);
  if (!tap.isValid()) {
    return false;
  }

  this._codec = (header.meta['avro.codec'] || 'null').toString();
  var codecs = this._codecs || BlockDecoder.getDefaultCodecs();
  this._decompress = codecs[this._codec];
  if (!this._decompress) {
    this._emitError(new Error(f('unknown codec: %s', this._codec)));
    return false;
  }

  var schemaString = header.meta['avro.schema'].toString();
  try {
    var schema = JSON.parse(schemaString);
    if (this._parseHook) {
      schema = this._parseHook(schema);
    }
    this._wType = types.Type.forSchema(schema);
  } catch (err) {
    this._emitError(err);
    return false;
  }

  try {
    this._readValue = createReader(this._noDecode, this._wType, this._rType);
  } catch (err) {
    this._emitError(err);
    return false;
  }

  // Bind the file identity and writer schema to the header. They are used to
  // reject checkpoints which don't belong to the file being read.
  this._syncMarker = header.sync;
  this._fileId = computeHash([
    header.sync,
    bufferFrom(schemaString),
    bufferFrom(this._codec)
  ]);
  this._schemaId = computeHash([bufferFrom(schemaString)]);
  this._mode = 'blocks';

  if (this._resume) {
    if (this._resume.fileId !== this._fileId) {
      this._emitError(new Error('checkpoint file mismatch'));
      return false;
    }
    if (this._resume.schema !== this._schemaId) {
      this._emitError(new Error('checkpoint schema mismatch'));
      return false;
    }
  }

  this.emit('metadata', this._wType, this._codec, header);
  return true;
};

BlockDecoder.prototype._write = function (chunk, encoding, cb) {
  var tap = this._tap;
  tap.buf = Buffer.concat([tap.buf, chunk]);
  tap.pos = 0;

  if (this._mode === 'error') {
    process.nextTick(cb);
    return;
  }

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
  var discarded = tap.pos;
  tap.buf = Buffer.concat([tap.buf.slice(discarded), chunk]);
  tap.pos = 0;
  this._tapBase += discarded; // Absolute offset of `tap.buf`'s first byte.

  if (this._mode === 'error') {
    process.nextTick(cb);
    return;
  }

  var nBlocks = 1;
  var block;
  while (true) {
    var blockPos = tap.pos; // Absolute offset within `tap.buf`.
    block = tryReadBlock(tap);
    if (!block) {
      break;
    }
    if (!this._syncMarker.equals(block.sync)) {
      this._emitError(new Error('invalid sync marker'));
      return;
    }

    var index = this._blockOrdinal++;
    var info = new BlockMeta(
      index,
      block.count,
      this._tapBase + blockPos,
      block.sync.toString('hex'),
      block.data.length,
      this._recordsBefore,
      fnv1a(block.data)
    );
    this._recordsBefore += block.count;
    this._blocks.push(info);

    if (this._resume && index < this._resume.block) {
      // Block entirely before the checkpoint, skip it without decompressing.
      continue;
    }

    var skip = 0;
    if (this._resume && index === this._resume.block) {
      if (!this._checkResumeTarget(info)) {
        return;
      }
      if (this._resume.index >= info.count - 1) {
        // The checkpoint is the last record of the block, skip it whole.
        this._acked = this._pushed = info.firstRecord + info.count;
        this._resumeDone = true;
        continue;
      }
      // Records `0..index` have been consumed already, skip them on read.
      skip = this._resume.index + 1;
      this._acked = this._pushed = info.firstRecord + skip;
      this._resumeDone = true;
    }

    nBlocks++;
    this._decompress(
      block.data,
      this._createBlockCallback(info, skip, chunkCb)
    );
  }
  chunkCb();

  function chunkCb() {
    if (!--nBlocks) {
      cb();
    }
  }
};

/** Check that a resumed block matches the checkpoint. */
BlockDecoder.prototype._checkResumeTarget = function (info) {
  var resume = this._resume;
  if (resume.offset !== undefined && resume.offset !== info.offset) {
    this._emitError(new Error('checkpoint offset mismatch'));
    return false;
  }
  if (resume.sync !== undefined && resume.sync !== info.sync) {
    this._emitError(new Error('checkpoint sync marker mismatch'));
    return false;
  }
  if (resume.compressed !== undefined && resume.compressed !== info.compressed) {
    this._emitError(new Error('checkpoint block size mismatch'));
    return false;
  }
  if (resume.checksum !== undefined && resume.checksum !== info.checksum) {
    this._emitError(new Error('checkpoint block checksum mismatch'));
    return false;
  }
  if (resume.index >= info.count) {
    this._emitError(new Error('checkpoint record index out of range'));
    return false;
  }
  if (resume.record >= 0 && resume.record !== info.firstRecord + resume.index) {
    this._emitError(new Error('checkpoint record index mismatch'));
    return false;
  }
  return true;
};

BlockDecoder.prototype._createBlockCallback = function (info, skip, cb) {
  var self = this;
  var index = this._index++;

  return function (cause, buf) {
    if (cause) {
      var err = new Error(f('%s codec decompression error', self._codec));
      err.cause = cause;
      self._emitError(err);
      cb();
      return;
    }

    if (self._mode === 'error') {
      cb();
      return;
    }

    if (skip > info.count) {
      // The block doesn't contain the checkpointed record.
      self._emitError(new Error('checkpoint mismatch'));
      cb();
      return;
    }

    self.emit('block', new BlockInfo(info.count, buf.length, info.compressed));
    self._queue.push(new BlockData(index, buf, cb, info.count, info, skip));
    if (self._needPush) {
      self._read();
    }
  };
};

BlockDecoder.prototype._read = function () {
  this._needPush = false;

  if (this._mode === 'error') {
    return;
  }

  var tap = this._blockTap;

  while (true) {
    if (!this._remaining) {
      var data = this._queue.pop();
      if (!data) {
        if (this._finished) {
          this.push(null);
        } else {
          this._needPush = true;
        }
        return; // Wait for more data.
      }
      data.cb();

      this._remaining = data.info.count - data.skip;
      tap.buf = data.buf;
      tap.pos = 0;
      if (data.skip && !this._skipRecords(tap, data.skip)) {
        return;
      }
      if (!this._remaining) {
        // Empty block or full block already committed.
        continue;
      }
    }

    // Decode and push a single record per call to avoid stalling the stream by
    // filling the readable buffer all at once.
    this._remaining--;
    var val;
    try {
      val = this._readValue(tap);
      if (!tap.isValid()) {
        throw new Error('truncated block');
      }
    } catch (err) {
      this._remaining = 0;
      this._emitError(err); // Corrupt data.
      return;
    }
    this._pushed++;
    if (this._objectMode) {
      this.push(val);
    } else {
      this._pushedBytes += val.length;
      this._recordEnds.push(this._pushedBytes);
      this.push(val);
      if (this._recordEnds.length > 4096) {
        // Bound memory usage for long-lived decoders by discarding record
        // boundaries which have already been acknowledged.
        this._getDelivered();
      }
    }
    return;
  }
};

/**
 * Skip the first `count` records of a block.
 *
 * The writer type's skipper is used, so no value is decoded. This is much
 * cheaper than decoding and discarding.
 */
BlockDecoder.prototype._skipRecords = function (tap, count) {
  var skipper = this._wType._skip;
  var i;
  try {
    for (i = 0; i < count; i++) {
      skipper(tap);
      if (!tap.isValid()) {
        throw new Error('truncated block');
      }
    }
  } catch (err) {
    this._remaining = 0;
    this._emitError(err);
    return false;
  }
  return true;
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
  this._queue = new utils.OrderedQueue();
  this._pending = 0;
  this._finished = false;
  this._needHeader = false;
  this._needPush = false;

  this._metadata = opts.metadata || {};
  if (!MAP_BYTES_TYPE.isValid(this._metadata)) {
    throw new Error('invalid metadata');
  }

  var codec = this._codec;
  this._compress = (this._codecs || BlockEncoder.getDefaultCodecs())[codec];
  if (!this._compress) {
    throw new Error(f('unsupported codec: %s', codec));
  }

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
    } else if (this._finished && this._needPush) {
      // We don't need to check `_isPending` since `_blockCount` is always
      // positive after the first flush.
      this.push(null);
    }
  });

  this.on('typeError', function (err) { this.emit('error', err); });
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
  this._compress(tap.buf.slice(0, pos), this._createBlockCallback(pos, cb));
  this._blockCount = 0;
};

BlockEncoder.prototype._read = function () {
  var self = this;
  var data = this._queue.pop();
  if (!data) {
    if (this._finished && !this._pending) {
      process.nextTick(function () { self.push(null); });
    } else {
      this._needPush = true;
    }
    return;
  }

  this.push(LONG_TYPE.toBuffer(data.count, true));
  this.push(LONG_TYPE.toBuffer(data.buf.length, true));
  this.push(data.buf);
  this.push(this._syncMarker);

  if (!this._finished) {
    data.cb();
  }
};

BlockEncoder.prototype._createBlockCallback = function (size, cb) {
  var self = this;
  var index = this._index++;
  var count = this._blockCount;
  this._pending++;

  return function (cause, data) {
    if (cause) {
      var err = new Error(f('%s codec compression error', self._codec));
      err.cause = cause;
      self.emit('error', err);
      return;
    }
    self._pending--;
    self.emit('block', new BlockInfo(count, size, data.length));
    self._queue.push(new BlockData(index, data, cb, count));
    if (self._needPush) {
      self._needPush = false;
      self._read();
    }
  };
};


// Helpers.

/** Summary information about a block. */
function BlockInfo(count, raw, compressed) {
  this.valueCount = count;
  this.rawDataLength = raw;
  this.compressedDataLength = compressed;
}

/**
 * An indexed block.
 *
 * This can be used to preserve block order since compression and decompression
 * can cause some some blocks to be returned out of order.
 *
 * `info` and `skip` are only used by the decoder: they carry the block's
 * metadata and the number of records to skip at the start of the block when
 * resuming from a checkpoint.
 */
function BlockData(index, buf, cb, count, info, skip) {
  this.index = index;
  this.buf = buf;
  this.cb = cb;
  this.count = count | 0;
  this.info = info;
  this.skip = skip | 0;
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

/**
 * Metadata about a block.
 *
 * `sync` is the hex encoding of the block's sync marker, which identifies it
 * uniquely together with the file's header. `firstRecord` is the number of
 * records preceding this block in the file. `compressed` is the length of the
 * compressed block data (excluding the count and sync marker).
 */
function BlockMeta(index, count, offset, sync, compressed, firstRecord, checksum) {
  this.index = index;
  this.count = count;
  this.offset = offset;
  this.sync = sync;
  this.compressed = compressed;
  this.firstRecord = firstRecord;
  this.checksum = checksum;
}

/**
 * Immutable record-level position within a container file.
 *
 * A position is bound to a specific file (via `fileId`) and writer schema (via
 * `schema`) so that it can be safely used to resume reading a different file
 * instance (for instance one reopened after an interruption). It is only ever
 * created once a record has been handed over to the consumer.
 *
 * The following properties are exposed:
 *
 *  + `fileId`: Hex fingerprint of the file header (sync marker, codec, and
 *    writer schema string).
 *  + `schema`: Hex fingerprint of the writer schema.
 *  + `block`: Index of the block containing the record.
 *  + `index`: Index of the record within its block.
 *  + `record`: Index of the record within the file (across all blocks).
 *  + `offset`: Absolute byte offset of the block containing the record.
 *  + `sync`: Hex encoded sync marker of the block containing the record.
 *  + `compressed`: Length of the block's compressed data.
 *  + `checksum`: Hex fingerprint of the block's compressed data.
 *
 * These are exactly the fields required to resume decoding after the record:
 * which block to restart from, how many records to skip in it, and how to
 * validate that the file and block are the expected ones.
 */
function Position(opts) {
  this.fileId = opts.fileId;
  this.schema = opts.schema;
  this.block = opts.block;
  this.index = opts.index;
  this.record = opts.record;
  this.offset = opts.offset;
  this.sync = opts.sync;
  this.compressed = opts.compressed;
  this.checksum = opts.checksum;
  Object.freeze(this);
}

Position.prototype.toJSON = function () {
  return {
    fileId: this.fileId,
    schema: this.schema,
    block: this.block,
    index: this.index,
    record: this.record,
    offset: this.offset,
    sync: this.sync,
    compressed: this.compressed,
    checksum: this.checksum
  };
};

Position.prototype.toString = function () {
  return JSON.stringify(this.toJSON());
};

Position.prototype.inspect = function () {
  return f('Position<%s>', this.toString());
};

/** Create a position. */
function createPosition(fileId, schema, block, index, record, offset, sync, compressed, checksum) {
  return new Position({
    fileId: fileId,
    schema: schema,
    block: block,
    index: index,
    record: record,
    offset: offset,
    sync: sync,
    compressed: compressed,
    checksum: checksum
  });
}

/** Validate and normalize a user-provided position. */
function normalizePosition(position) {
  if (position instanceof Position) {
    return position;
  }
  if (typeof position === 'string') {
    try {
      position = JSON.parse(position);
    } catch (err) {
      throw new Error('invalid position');
    }
  }
  if (!position || typeof position !== 'object') {
    throw new Error('invalid position');
  }

  var block = position.block;
  var index = position.index;
  if (typeof block !== 'number' || block < 0 || block % 1) {
    throw new Error('invalid position block');
  }
  if (typeof index !== 'number' || index < 0 || index % 1) {
    throw new Error('invalid position index');
  }

  var fileId = position.fileId;
  var schema = position.schema;
  if (typeof fileId !== 'string' || typeof schema !== 'string') {
    throw new Error('invalid position fingerprint');
  }

  var offset = position.offset;
  if (offset !== undefined && (typeof offset !== 'number' || offset < 0 || offset % 1)) {
    throw new Error('invalid position offset');
  }
  var sync = position.sync;
  if (sync !== undefined && typeof sync !== 'string') {
    throw new Error('invalid position sync marker');
  }
  var compressed = position.compressed;
  if (compressed !== undefined &&
      (typeof compressed !== 'number' || compressed < 0 || compressed % 1)) {
    throw new Error('invalid position compressed length');
  }
  var checksum = position.checksum;
  if (checksum !== undefined && typeof checksum !== 'string') {
    throw new Error('invalid position checksum');
  }

  return createPosition(
    fileId,
    schema,
    block,
    index,
    position.record === undefined ? -1 : position.record,
    offset,
    sync,
    compressed,
    checksum
  );
}

/**
 * Compute a hex fingerprint from buffers.
 *
 * The buffers are hex encoded before hashing so that the computation is safe
 * both in node and in the browser (where the crypto shim only accepts strings).
 */
function computeHash(bufs) {
  return utils.getHash(Buffer.concat(bufs).toString('hex')).toString('hex');
}

/**
 * Fast 32-bit FNV-1a hash of a buffer, hex encoded (with length).
 *
 * The multiplication is split to stay within 32 bits without relying on
 * `Math.imul` (which isn't available in all runtimes supported by this
 * library).
 */
function fnv1a(buf) {
  var h = 0x811c9dc5;
  var lo, hi;
  for (var i = 0, l = buf.length; i < l; i++) {
    h ^= buf[i];
    // h *= 0x01000193 (mod 2^32).
    lo = h & 0xffff;
    hi = h >>> 16;
    h = (lo * 0x01000193 + ((hi * 0x01000193) << 16)) >>> 0;
  }
  // Include the length to distinguish buffers with trailing zero bytes.
  return (h >>> 0).toString(16) + '-' + buf.length.toString(16);
}

/** Convert a string to a buffer (helper for hashing). */
function bufferFrom(str) {
  return utils.bufferFrom(str);
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
