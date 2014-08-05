# fd-slicer

Safe `fs.ReadStream` and `fs.WriteStream` using the same fd.

Let's say that you want to perform a parallel upload of a file to a remote
server. To do this, we want to create multiple read streams. The first thing
you might think of is to use the `{start: 0, end: 0}` API of
`fs.createReadStream`. This gives you two choices:

 0. Use the same file descriptor for all `fs.ReadStream` objects.
 0. Open the file multiple times, resulting in a separate file descriptor
    for each read stream.

Neither of these are acceptable options. The first one is a severe bug,
because the API docs for `fs.write` state:

> Note that it is unsafe to use `fs.write` multiple times on the same file
> without waiting for the callback. For this scenario, `fs.createWriteStream`
> is strongly recommended.

`fs.createWriteStream` will solve the problem if you only create one of them
for the file descriptor, but it will exhibit this unsafety if you create
multiple write streams per file descriptor.

The second option suffers from a race condition. For each additional time the
file is opened after the first, it is possible that the file is modified. So
in our parallel uploading example, we might upload a corrupt file that never
existed on the client's computer.

This module solves this problem by providing `createReadStream` and
`createWriteStream` that operate on a shared file descriptor and provides
the convenient stream API while still allowing slicing and dicing.

## Usage

```js
var FdSlicer = require('fd-slicer');
var fs = require('fs');

fs.open("file.txt", 'r', function(err, fd) {
  if (err) throw err;
  var fdSlicer = new FdSlicer(fd);
  var firstPart = fdSlicer.createReadStream({start: 100});
  var secondPart = fdSlicer.createReadStream({start: 0, end: 100});
  var firstOut = fs.createWriteStream("first.txt");
  var secondOut = fs.createWriteStream("second.txt");
  firstPart.pipe(firstOut);
  secondPart.pipe(secondOut);
});

fdSlicer.create();
```
