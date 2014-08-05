var FdSlicer = require('../');
var fs = require('fs');
var crypto = require('crypto');
var path = require('path');
var streamEqual = require('stream-equal');
var assert = require('assert');
var Pend = require('pend');

var describe = global.describe;
var it = global.it;
var before = global.before;
var after = global.after;

var testBlobFile = path.join(__dirname, "test-blob.bin");
var testBlobFileSize = 20 * 1024 * 1024;

describe("FdSlicer", function() {
  before(function(done) {
    var out = fs.createWriteStream(testBlobFile);
    for (var i = 0; i < testBlobFileSize / 1024; i += 1) {
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
  it("reads 4 chunks simultaneously", function(done) {
    fs.open(testBlobFile, 'r', function(err, fd) {
      if (err) return done(err);
      var fdSlicer = new FdSlicer(fd);
      var actualPart1 = fdSlicer.createReadStream({start: testBlobFileSize * 0/4});
      var actualPart2 = fdSlicer.createReadStream({start: testBlobFileSize * 1/4});
      var actualPart3 = fdSlicer.createReadStream({start: testBlobFileSize * 2/4});
      var actualPart4 = fdSlicer.createReadStream({start: testBlobFileSize * 3/4});
      var expectedPart1 = fdSlicer.createReadStream({start: testBlobFileSize * 0/4});
      var expectedPart2 = fdSlicer.createReadStream({start: testBlobFileSize * 1/4});
      var expectedPart3 = fdSlicer.createReadStream({start: testBlobFileSize * 2/4});
      var expectedPart4 = fdSlicer.createReadStream({start: testBlobFileSize * 3/4});
      var pend = new Pend();
      pend.go(function(cb) {
        streamEqual(expectedPart1, actualPart1, function(err, equal) {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(function(cb) {
        streamEqual(expectedPart2, actualPart2, function(err, equal) {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(function(cb) {
        streamEqual(expectedPart3, actualPart3, function(err, equal) {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.go(function(cb) {
        streamEqual(expectedPart4, actualPart4, function(err, equal) {
          assert.ok(equal);
          cb(err);
        });
      });
      pend.wait(function(err) {
        if (err) return done(err);
        fs.close(fd);
        done();
      });
    });
  });
});
