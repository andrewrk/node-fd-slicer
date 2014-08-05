var fs = require('fs');
var util = require('util');
var stream = require('stream');
var Readable = stream.Readable;
var Writable = stream.Writable;
var Pend = require('pend');

exports.createContext = createContext;

function createContext(fd) {
  return new Context(fd);
}

function Context(fd) {
  this.fd = fd;
  this.pend = new Pend();
  this.pend.max = 1;
}

Context.prototype.createReadStream = function(options) {
  return new ReadStream(this, options);
};

Context.prototype.createWriteStream = function(options) {
  return new WriteStream(this, options);
};

util.inherits(ReadStream, Readable);
function ReadStream(context, options) {
  Readable.call(this, options);

  this.context = context;

  this.start = options.start || 0;
  this.end = options.end;
  this.pos = this.start;
  this.destroyed = false;
  this.buffer = new Buffer(this._readableState.highWaterMark);
}

ReadStream.prototype._read = function(n) {
  var self = this;
  if (self.destroyed) return;

  var toRead = Math.min(self.buffer.length, n);
  if (self.end != null) {
    toRead = Math.min(toRead, self.end - self.pos);
  }
  if (toRead <= 0) {
    self.push(null);
    return;
  }

  self.context.pend.go(function(cb) {
    if (self.destroyed) return cb();
    fs.read(self.context.fd, self.buffer, 0, toRead, self.pos, function(err, bytesRead) {
      cb();
      if (err) {
        self.destroy();
        self.emit('error', err);
      } else if (bytesRead === 0) {
        self.push(null);
      } else {
        self.pos += bytesRead;
        self.push(self.buffer.slice(0, bytesRead));
      }
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

  self.context.pend.go(function(cb) {
    if (self.destroyed) return cb();
    fs.write(self.context.fd, data, 0, data.length, this.pos, function(err, bytes) {
      cb();
      if (err) {
        self.destroy();
        callback(err);
      } else {
        self.bytesWritten += bytes;
        self.pos += data.length;
        callback();
      }
    });
  });
};

WriteStream.prototype.destroy = function() {
  this.destroyed = true;
};
