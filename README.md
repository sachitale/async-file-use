Core Java: How to use AsynchronousFileChannel and the attachment facility it comes with.

JDK 7 onward java has given great facilities to write Async code. This small post demonstrates how to use AsynchronousFileChannel and how to effectively use the attachment. Most of the articles I've seen stop short of explaining how to use the attachment facility.

How to compile:
javac -d bin src/me/nachis/async/AsyncFileWriter.java

How to execute:
java -cp bin me.nachis.async.AsyncFileWriter <filename.ext>

