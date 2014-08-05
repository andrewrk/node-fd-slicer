var FdSlicer = require('../');
var fs = require('fs');
var crypto = require('crypto');
var path = require('path');
var streamEqual = require('stream-equal');
var assert = require('assert');

var describe = global.describe;
var it = global.it;
var before = global.before;
var after = global.after;

var testBlobFile = path.join(__dirname, "test-blob.bin");

describe("FdSlicer", function() {
  before(function(done) {
    var out = fs.createWriteStream(testBlobFile);
    for (var i = 0; i < 20 * 1024; i += 1) {
      out.write(crypto.pseudoRandomBytes(1024));
    }
    out.end();
    out.on('close', done);
  });
  after(function(done) {
    fs.unlink(testBlobFile, function(err) {
      done();
    });
  });
  it("reads a 20MB file", function(done) {
    fs.open(testBlobFile, 'r', function(err, fd) {
      if (err) return done(err);
      var fdSlicer = new FdSlicer(fd);
      var actualStream = fdSlicer.createReadStream();
      var expectedStream = fs.createReadStream(testBlobFile);

      streamEqual(expectedStream, actualStream, function(err, equal) {
        if (err) return done(err);
        assert.ok(equal);
        fs.close(fd, done);
      });
    });
  });
});
