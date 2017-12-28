package me.nachis.async;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This simple program demonstrates how to use Async File Channel to write a big file
 * @author sachin
 * 
 * USAGE:
 * java -cp bin me.nachis.async.AsyncFileWriter <filename.ext>
 *
 */
public class AsyncFileWriter {
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		File f = new File(args[0]);
		Path p = f.toPath();
		AsynchronousFileChannel af = AsynchronousFileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

		Attachment attachment = new Attachment();
		// The attachment object contains all the resources required by the file channel
		// to operate on its own with in the completion handler
		//
		// Here we are providing:
		// 1. The Async file channel on which the subsequent operations will happen
		//
		// 2. The initial file position (This is the most important part)
		//
		// 3. A dummy data source, you can replace this with your own data source
		//
		// 4. A synchronization mechanism for stand alone program like this to communicate 
		//    with Async channel to indicate completion of operation.
		//
		// Point 4 is only a requirement for stand alone programs, in the 
		// absence of which main method will exit before the Asyn File Channel
		// thread could complete its work.
		//
		
		attachment.fc = af;
		attachment.filePosition = 0;
		attachment.ds = new DummyDataSource();
		
		attachment.ds.createDummyData(); // create some data to start

		// use dummy datasource's utility method to create a ByteBuffer
		byte[] data = attachment.ds.data_q.poll();
		// System.out.println(data.length);
		ByteBuffer buf = ByteBuffer.wrap(data);
		
		// At this point a new thread pool is created and all the Async work is done in that thread pool.
		af.write(buf, attachment.filePosition, attachment, new SimpleCompletionHandler());
		
		// We need to wait here, until Asyn File Channel thread calls
		// countdown on the latch
		// If we dont call await the main thread will exit and incomplete file may be written to the disk

		// create dummy data in main thread
		int LOOP = attachment.ds.r.nextInt(2048);
		System.out.printf("No of times the data loop with run (Each data enrty can be .5 MB long) %d\n",LOOP);
		for(int i=0; i<LOOP; ++i) {
			attachment.ds.createDummyData();
		}
		//System.out.println("done");
		// blank data signals EOF
		attachment.ds.createBlankData();
		
		attachment.fileWriteComplete.await();
	}

	private static class DummyDataSource {
		LinkedBlockingQueue<byte[]> data_q = new LinkedBlockingQueue<>();
		Random r = new Random(System.currentTimeMillis());
		DummyDataSource() {
			// init with 10 strings
		}
		
		void createDummyData() {
			int len = r.nextInt(1024*512);
			byte[] bytes = new byte[len];
			Arrays.fill(bytes, (byte)'A');
			data_q.add(bytes);
		}
		void createBlankData() {
			byte[] bytes = new byte[0];
			data_q.add(bytes);
		}

	}

	/**
	 * Attachment holds all the required info for async channel to operate.
	 * Most importantly it keeps track of the file position, where data will be written to.
	 *
	 */
	private static class Attachment {
		long filePosition;
		AsynchronousFileChannel fc;
		DummyDataSource ds;
		
		// 
		// you can use any synchronization mechanism
		CountDownLatch fileWriteComplete = new CountDownLatch(1);
	}
	
	/**
	 * Purpose of this Completion Handler is:
	 * After the completion of previous write of data:
	 * 1. Increment the postion where next to write by number of bytes writter
	 * 2. Take any new data in the buffer, write it using AsyncFileChannel handle.
	 * 
	 * Subsequent runs will see the filePosition change like this for a data of 167 bytes:
	 * 0 : initial
	 * 0 + 64
	 * 64 + 64
	 * 128 + 39 (eof) 
	 */
	private static class SimpleCompletionHandler implements CompletionHandler<Integer, Attachment>
	{

		@Override
		public void completed(Integer bytesWritten, Attachment att) {
			try {
				// take the bytes written and add it to the current count
				att.filePosition += bytesWritten;
				
				// assume that every 100ms data will be available
				byte[] data = att.ds.data_q.poll(100, TimeUnit.MILLISECONDS);

				ByteBuffer moreData = ByteBuffer.wrap( data );
				// A zero length data is considered end of stream
				if(moreData.capacity() == 0) {
					att.fc.force(true);
					att.fc.close();
					att.fileWriteComplete.countDown();
				}
				else {
					// now take the new position and use it
					att.fc.write(moreData, att.filePosition, att, this);
				}
			}
			catch( IOException | InterruptedException ex) {
				ex.printStackTrace();
				throw new RuntimeException(ex);
			}
		}
		
		@Override
		public void failed(Throwable e, Attachment att) {
			e.printStackTrace();
			// handle exception here
		}
	}
}
