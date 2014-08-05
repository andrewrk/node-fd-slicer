var fs = require('fs');
var util = require('util');
var stream = require('stream');
var Readable = stream.Readable;
var Writable = stream.Writable;
var Pend = require('pend');

module.exports = FdSlicer;

function FdSlicer(fd) {
  this.fd = fd;
  this.pend = new Pend();
  this.pend.max = 1;
}

FdSlicer.prototype.createReadStream = function(options) {
  return new ReadStream(this, options);
};

FdSlicer.prototype.createWriteStream = function(options) {
  return new WriteStream(this, options);
};

util.inherits(ReadStream, Readable);
function ReadStream(context, options) {
  options = options || {};
  Readable.call(this, options);

  this.context = context;

  this.start = options.start || 0;
  this.end = options.end;
  this.pos = this.start;
  this.destroyed = false;
}

ReadStream.prototype._read = function(n) {
  var self = this;
  if (self.destroyed) return;

  var toRead = Math.min(self._readableState.highWaterMark, n);
  if (self.end != null) {
    toRead = Math.min(toRead, self.end - self.pos);
  }
  if (toRead <= 0) {
    self.push(null);
    return;
  }
  var buffer = new Buffer(toRead);

  self.context.pend.go(function(cb) {
    if (self.destroyed) return cb();
    fs.read(self.context.fd, buffer, 0, toRead, self.pos, function(err, bytesRead) {
      if (err) {
        self.destroy();
        self.emit('error', err);
      } else if (bytesRead === 0) {
        self.push(null);
      } else {
        self.pos += bytesRead;
        self.push(buffer.slice(0, bytesRead));
      }
      cb();
    });
  });
};

ReadStream.prototype.destroy = function() {
  this.destroyed = true;
};

util.inherits(WriteStream, Writable);
function WriteStream(context, options) {
  options = options || {};
  Writable.call(this, options);

  this.context = context;
  this.start = options.start || 0;
  this.bytesWritten = 0;
  this.pos = this.start;
  this.destroyed = false;
}

WriteStream.prototype._write = function(data, encoding, callback) {
  var self = this;
  if (self.destroyed) return;

  var buffer;
  if (self.context.pend.pending) {
    buffer = new Buffer(data.length);
    data.copy(buffer);
  } else {
    buffer = data;
  }
  self.context.pend.go(function(cb) {
    if (self.destroyed) return cb();
    fs.write(self.context.fd, buffer, 0, buffer.length, self.pos, function(err, bytes) {
      if (err) {
        self.destroy();
        cb();
        callback(err);
      } else {
        self.bytesWritten += bytes;
        self.pos += bytes;
        cb();
        callback();
      }
    });
  });
};

WriteStream.prototype.destroy = function() {
  this.destroyed = true;
};
