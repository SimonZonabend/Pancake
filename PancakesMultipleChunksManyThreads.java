
package test;

import static java.lang.Integer.MAX_VALUE;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exhaustive graph search program for the Pancake Sorting Problem
 * 
 * Parallel TBBFS, expanding multiple chunks in memory in parallel.
 * 
 * Using a TBBFS - Two bit breadth first search it is possible to 
 * walk through all possible pancakes stacks and then finding the maximum branch.
 * Ideas taken from: https://www.aaai.org/Papers/AAAI/2008/AAAI08-050.pdf
 * 
 * 
 * A pancake stack is encoded using a lexicographic number.
 * So pancake stack 876543210 is number 0, stack 876543201 is number 1 etc...
 * This way a single bit can be used to represent a full stack of pancakes, 
 * but the graph search needs another bit to note which stacks are the current and next, 
 * so in total two bits are used.
 * 
 * Threads are used to split up the work.
 * If 8 GB of memory is used to represent the graph and 4 threads used, each thread 
 * will be responsible for a 2 GB chunk. Any nodes resident in another chunk must be written to disk.
 * 
 * If the graph doesn't fit in memory,  disk storage will be used together with MemoryMappedFiles.
 * The graph itself is stored on disk named CHUNK-file (CHUNK0, CHUNK1... CHUNKN) with two-bit per state.   
 * Each thread will store its own NEXT/CURRENT files with the nodes to visit.
 * The nodes are stored as a 5-byte big endian encoded long.
 * 
 * 
 * The basic processing is simple, each thread will: 
 * 1) Load a chunk to its memory.
 * 2) Merge all its nodes from NEXT and CURRENT FILES.  
 * 3) Breadth first search this chunk and store nodes to current chunk to NEXT/CURRENT files. 
 * 4) Store chunk to disk.
 * 5) Goto 1... until all nodes have been visited.
 * 
 *   
 * USAGE:
 * arg 1: PANCAKE NUMBER=X-Y  default 1-99
 * arg 2: BYTES_IN_MEMORY=x   default max memory.
 * arg 3: THREADS=x           default number of available processors
 * 
 * Example: 
 * java -jar pancakes.jar  
 * Calculate a stack of pancakes using all memory available and all cpus available.
 
 * Example: 
 * java -jar pancakes.jar 5  
 * Calculate a stack of 5 pancakes using all memory available and all cpus available.
 *
 * Example: java -jar pancakes.jar 5 24
 * Calculate a stack of 5 pancakes using 24 bytes of memory (5 chunks will be stored on disk)
 * 
 * Example: java -jar pancakes.jar 5 24 1 
 * Calculate a stack of 5 pancakes using 24 bytes memory (5 chunks will be stored on disk) with only one thread.
 * 
 * 
 * Typical run.
 * Pancake 11: vertexes=39916800 chunks=4 threads=4 memory usage 9 MB
 * LEVEL 0 NODES=1
 * LEVEL 1 NODES=10
 * LEVEL 2 NODES=90
 * LEVEL 3 NODES=809
 * LEVEL 4 NODES=6429
 * LEVEL 5 NODES=43891
 * LEVEL 6 NODES=252737
 * LEVEL 7 NODES=1174766
 * LEVEL 8 NODES=4126515
 * LEVEL 9 NODES=9981073
 * LEVEL 10 NODES=14250471
 * LEVEL 11 NODES=9123648
 * LEVEL 12 NODES=956354
 * LEVEL 13 NODES=6
 * LAST NODES:
 * [0, 10, 2, 7, 4, 9, 6, 3, 8, 5, 1]
 * [0, 10, 2, 5, 8, 3, 6, 9, 4, 7, 1]
 * [0, 6, 1, 10, 3, 5, 8, 4, 7, 9, 2]
 * [0, 2, 10, 4, 7, 5, 1, 8, 6, 9, 3]
 * [6, 1, 8, 5, 10, 3, 0, 9, 2, 7, 4]
 * [0, 4, 2, 9, 1, 5, 8, 10, 6, 3, 7]
 * Time: 11237 millis. Used memory: 11,26 MB
 *
 * Pancake 14: vertexes=87178291200 chunks=11 threads=4 memory usage 7558 MB
 * LEVEL 0 NODES=1
 * LEVEL 1 NODES=13
 * LEVEL 2 NODES=156
 * LEVEL 3 NODES=1871
 * LEVEL 4 NODES=20703
 * LEVEL 5 NODES=206681
 * LEVEL 6 NODES=1858149
 * LEVEL 7 NODES=14721545
 * LEVEL 8 NODES=100464346
 * LEVEL 9 NODES=572626637
 * LEVEL 10 NODES=2609061935
 * LEVEL 11 NODES=8950336881
 * LEVEL 12 NODES=21189628403
 * LEVEL 13 NODES=30330792508
 * LEVEL 14 NODES=20584311501
 * LEVEL 15 NODES=2824234896
 * LEVEL 16 NODES=24974
 * LAST NODES:
 * [13, 7, 12, 3, 11, 5, 9, 1, 8, 6, 10, 4, 2, 0]
 */
class PancakesMultipleChunksManyThreads
{
    byte[] pancakes;
    int pancakeLength;
	long[] factorials;    
	int numberOfChunks;
 
	long NUMBER_OF_BYTES_ALLOWED = Runtime.getRuntime().maxMemory();
	int THREADS = Runtime.getRuntime().availableProcessors();
	int PANCAKE_START = 1;
	int PANCAKE_END = 99;

	static final int PAGE_SIZE = 16*1024;
	static final boolean DEBUG = false;        

    RandomAccessFile[] raf;

	ExecutorService executorService;
	
	public static byte NOT_VISITED    = 0;
	public static byte CURRENT_LEVEL  = 1; 
	public static byte NEXT_LEVEL     = 2;
	public static byte FINISHED_LEVEL = 3;
	
	List<MyWorker> callables;
	
    public static void main (String args[])  
    {
    	try {
    		new PancakesMultipleChunksManyThreads(args);
    	} catch(OutOfMemoryError e) {
    		System.out.println("Add more heap space (-Xmx) or decrease the number of bytes allowed in memory.");
    	}
    }

    static class ByteArray64 {
    	static final int CHUNK_SIZE = Integer.MAX_VALUE - 8;
        long size;
        byte [][] data;
        BufferIterator iterator; 
        
        public ByteArray64( long size ) {
        	iterator = new BufferIterator();
            this.size = size;
            if( size == 0 ) {
                data = null;
            } else {
                int chunks = (int)(size/CHUNK_SIZE);
                int remainder = (int)(size - ((long)chunks)*CHUNK_SIZE);
				data = new byte[chunks + (remainder == 0 ? 0 : 1)][];
                for( int idx=chunks; --idx>=0; ) {
                    data[idx] = new byte[(int)CHUNK_SIZE];
                }
                if( remainder != 0 ) {
                    data[chunks] = new byte[remainder];
                }
            }
        }

        public long size() {
            return size;
        }

		public void setAll(byte from, byte to) {
			for(int c=0; c<data.length; c++) {
				int offsets = data[c].length;
				for(int offset=0; offset<offsets; offset++) {
					byte b = data[c][offset];
					if(((b) & FINISHED_LEVEL) == from) {
						b |= to;
					}
					if(((b>>2) & FINISHED_LEVEL) == from) {
						b |= (to<<2);
					}
					if(((b>>4) & FINISHED_LEVEL) == from) {
						b |= (to<<4);
					}
					if(((b>>6) & FINISHED_LEVEL) == from) {
						b |= (to<<6);
					}
					data[c][offset] = b;
				}
			}
		}

		public boolean setIfNotVisited(long lexNumber, byte bit) {
			long bytePosition = lexNumber >> 2; 
			int chunk = (int) (bytePosition / CHUNK_SIZE);
			int offset = (int) (bytePosition - (((long) chunk) * CHUNK_SIZE));
            int remainder = (int) (lexNumber & 3) << 1; // = lexNumber % 4 
            byte b = (byte)((data[chunk][offset] >> remainder) & FINISHED_LEVEL);

        	if(b == NOT_VISITED) {
	        	data[chunk][offset] |= (byte)(bit << remainder);
	        	return true;
        	}
        	return false;
		}
	

		public long getNextLevel(byte level) {
			while(true) {
				if(iterator.lexNumber > iterator.endLexNumber) {
					return -1;
				}

				byte b = data[iterator.chunk][iterator.bytePos];
				int remainder = (int) (iterator.lexNumber & 3);  // iterator.lexNumber % 4
				
				if(remainder < 1) {
					iterator.lexNumber++;
					if((b & FINISHED_LEVEL) == level) {
						break;
					}
				} 
				if(remainder < 2)
				{
					iterator.lexNumber++;
					if(((b>>2) & FINISHED_LEVEL) == level) {
						break;
					}
				} 
				if(remainder < 3)
				{
					iterator.lexNumber++;
					if(((b>>4) & FINISHED_LEVEL) == level) {
						break;
					}
				} 		
				
				iterator.lexNumber++;
				iterator.bytePos++;
				if(iterator.bytePos == CHUNK_SIZE) {
					iterator.chunk++;
					iterator.bytePos = 0;
				}
				if(((b>>6) & FINISHED_LEVEL) == level) {
					break;
				}
			}
			
			return iterator.lexNumber - 1;
		}
		
        public void startIterating(long endLexNumber) {

			iterator.chunk = 0;
			iterator.lexNumber = 0;
	        		
			iterator.endLexNumber = endLexNumber;
			iterator.bytePos = 0;
        }


		public void setNext(byte b) {
			data[iterator.chunk][iterator.bytePos++] = b;
			if(iterator.bytePos == CHUNK_SIZE) {
				iterator.chunk++;
				iterator.bytePos = 0;
			}
		}
		
		public byte getNext() {
			if(iterator.bytePos == CHUNK_SIZE) {
				iterator.chunk++;
				iterator.bytePos = 0;
			}
			byte b = data[iterator.chunk][iterator.bytePos++];
			
			return b;
		}
    }

    static class BufferIterator {
		int bytePos;
		long lexNumber;
		int chunk;
        int endChunk;
        long endLexNumber;
    }
    
    /**
     * Encodes as long using 4 bits per digit.
     * Will cover at most 16 pancakes (and one implicit).
     * 
     * @param arr
     * @return
     */
    long encodeToLong(byte[] arr) {
    	long number = arr[0];
		
		for(int i=1; i<arr.length; i++) {
			number <<= 4;
			number += arr[i];
		}		
		return number;
    }
    
    /**
     * Decodes a long number encoded as 4 bit per digit to a byte array.
     * @param number
     * @param out
     */
    void decodeLong(long number, byte[] out) {
    	int digit = out.length - 1;
   		while(digit >= 0) {
   			out[digit--] = (byte)(number & 0x0F);
   			number >>=4;
   		}
    }

    void setArgs(String[] args) {
    	try {
    		if(args.length == 0) return;
    		
    		String[] a = args[0].split("-");
			if (a.length > 0) {
				PANCAKE_START = Integer.parseInt(a[0]);
				if (a.length > 1) {
					PANCAKE_END = Integer.parseInt(a[1]);
				} else {
					PANCAKE_END = PANCAKE_START + 1;
				}
			}
			if (args.length > 1) {
				NUMBER_OF_BYTES_ALLOWED = Math.min(Long.parseLong(args[1]), Runtime.getRuntime().maxMemory());
			} 
			if (args.length > 2) {
				THREADS = Integer.parseInt(args[2]);
			} 
	    } catch(Exception e) {
	  		 System.out.println("Exhaustive search program for the Pancake Sorting Problem");
	  		 System.out.println("Example: 11 39916800 4");
	  		 System.out.println("arg 1: PANCAKE NUMBER=X-Y  default 1-99");
	  		 System.out.println("arg 2: BYTES_IN_MEMORY=x   default max memory.");
	  		 System.out.println("arg 3: THREADS=x           default number of available processors");
	  		 System.exit(0);
	    }
    }
    
    public PancakesMultipleChunksManyThreads(String[] args) {
/*    	 try {
			System.setOut(new PrintStream(new File("output-file.txt")));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
  */	 
    	
    	Runtime rt = Runtime.getRuntime();
    	long startTime;
    	setArgs(args);

    	System.out.println("Max mem:" + rt.maxMemory());
    
    	for(pancakeLength=PANCAKE_START; pancakeLength<PANCAKE_END; pancakeLength++) { 
    		startTime = System.currentTimeMillis();
    		long startMem = rt.totalMemory() - rt.freeMemory();
    		
    		factorials = new long[pancakeLength];
    		pancakes = new byte[pancakeLength];  
    				
	    	for(byte i = 0; i<pancakeLength; i++) {
	    		pancakes[i] = i;
	    		factorials[i] = factorial(i);
	    	}

	    	// 
	    	// ALLOCATE ARRAY FOR ALL POSSIBLE STACKS OF PANCAKES.
	    	// 
	    	long vertexes = factorial(pancakeLength);
	    	long bytesInMemory = 0;
	    	
	    	long totalBytesRequired = (vertexes + 3) / 4;
	    	
	    	if(NUMBER_OF_BYTES_ALLOWED >= totalBytesRequired) {
	    		bytesInMemory = totalBytesRequired;
	    	} else if(totalBytesRequired % NUMBER_OF_BYTES_ALLOWED != 0){
	    		// must be a multiple of vertexes so we can calculate chunks.
	    		int chunks = (int)(totalBytesRequired / NUMBER_OF_BYTES_ALLOWED);
	    		bytesInMemory = (long)(totalBytesRequired / (chunks + 1));
	    	} else {
	    		bytesInMemory = NUMBER_OF_BYTES_ALLOWED;
	    	}
	    	
	    	if(bytesInMemory > 0xFFFFFFFFFFL) {
	    		throw new RuntimeException("Need to increase child size");
	    	}
	    	if(pancakeLength > 16) {
	    		throw new RuntimeException("Need to increase internal pancake format (long)");
	    	}	
	    	
    		long bytesPerCPU = (bytesInMemory + THREADS - 1) / THREADS;
    		numberOfChunks = (int) ((totalBytesRequired + bytesPerCPU - 1) / bytesPerCPU); 	

    		// Create chunk files
    		removeOldFiles();
	    	 
    		raf = new RandomAccessFile[numberOfChunks];
    		if(numberOfChunks > 1) {
				for(int chunk=0; chunk<numberOfChunks; chunk++) {
	    			try {
	    				raf[chunk] = new RandomAccessFile("CHUNK" + chunk, "rw");
	    				raf[chunk].setLength(bytesPerCPU);
						raf[chunk].getFD().sync();
	    			} catch (FileNotFoundException e) {
	    				e.printStackTrace();
	    			} catch (IOException e) {
						e.printStackTrace();
					}
	    		}
			}
    		

    	   	//
	    	// INITIALIZE THREADS.
	    	//

        	initExecutorService(numberOfChunks);
        	
			for(int i=0; i<callables.size(); i++) {
				callables.get(i).init(i, bytesPerCPU);
			}					

	    	System.out.println("Pancake " + pancakeLength + ": vertexes=" + vertexes + " chunks=" + numberOfChunks + " threads=" + callables.size() + " memory usage " + bytesInMemory / 1024 / 1024 +" MB");

	    	//
	    	// Breadth first graph search.
	    	//
	    	try {
				bfs(vertexes);
			} catch (Exception e) {
				e.printStackTrace();
			} 
	    	
	    	// Release memory mapped files.
	    	if(numberOfChunks > 1) {
				for (int c = 0; c < numberOfChunks; c++) {
					try {
			            raf[c].getChannel().close();
						raf[c].close();
						for(int t=0; t<callables.size(); t++) {
							callables.get(t).childNodeFilesCurrent[c].close();
							callables.get(t).childNodeFilesNext[c].close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
	    	}
	    	System.out.printf("Time: %d millis. Used memory: %.2f MB", (System.currentTimeMillis() - startTime), (((rt.totalMemory() - rt.freeMemory()) - startMem) / 1048576.0));
	    	System.out.println("\n");
	    	removeOldFiles();
	    	executorService.shutdown();
    	}
    	
    }
 
    private void clean(MappedByteBuffer mappedBuffer) {
    	if (mappedBuffer instanceof sun.nio.ch.DirectBuffer) {
		    sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) mappedBuffer).cleaner();
		    cleaner.clean();
    	}	
    }    

    private void removeOldFiles() {
    	File f; 
		for(int i=0; i<=numberOfChunks; i++) {
   			for(int id=0; id<THREADS; id++) {   				
    			String name = "T" + id + "L" + pancakeLength + "C" + i+"-CURRENT"; 
	    		f = new File(name);
	    		if(f.exists()) {
	    			if(!f.delete()) {
	    				System.err.println("Failed to remove " + f.getName());
	    			}
	    		}
	    		name = "T" + id + "L" + pancakeLength + "C" + i+"-NEXT"; 
	    		f = new File(name);
	    		if(f.exists()) {
	    			if(!f.delete()) {
	    				System.err.println("Failed to remove " + f.getName());
	    			}
	    		}
    		}
    		f = new File("CHUNK" + i);
    		if(f.exists()) {
    			if(!f.delete()) {
    				System.err.println("Failed to remove " + f.getName());
    			}
    		}
		}
    }

	/** Reverses a long where each pancake consists of 4 bits
     * @param arr
     * @param i which pancake to flip,
     * @return
     */
    long flip(long arr, int i, int length)
    {
        long outcome = arr;
        int bitDist = i * 4;
        long highmask = (0x0FL << bitDist);
        long lowmask = 0x0FL;

        while (highmask > lowmask)
        {
            long low = arr & lowmask;
            long high = arr & highmask;
            outcome = (outcome - low - high) + (low << bitDist) + (high >> bitDist);
            bitDist -= 8;
            lowmask <<= 4;
       		highmask >>= 4;
        }

        return outcome;
    }  

	long factorial(int n) {
		long fact = 1;
		for (int i = 1; i <= n; i++) {
			fact *= i;
		}
		return fact;
	}
	
	/**
    * Gets the lexical number from a pancake stack.
	* To map a permutation to a sequence of factorial digits, we
	* subtract from each element the number of original elements
	* to its left that are less than it.
	* 
     * @param number A pancake stack encoded as long with 4 bit per pancake.
     * @return
     */
    long toLexicalNumber(long number) {
       	int bitUsed = 0;
       	long lexNumber = 0;
       	int pos = pancakeLength - 1;
       	
       	int n = (int) (number >> (pos * 4));
       	lexNumber = factorials[pos] * n; 
       	bitUsed = (1 << n);
   		
       	while (--pos > 0)
       	{
     		n = (int)((number >> (pos * 4)) & 0x0F) ;
       		int numbersLeftLess = Integer.bitCount(bitUsed << (31-n));
       		lexNumber += (n - numbersLeftLess) * factorials[pos];
       		bitUsed |= (1 << n);
       	}
       	return lexNumber;
    }
   
    /**
     * Converts a lexicographic number to a pancake stack encoded as a long (4 bits per pancake)
     * not in linear time..
     * @param lexNumber
     * @return
     */
    long fromLexicalNumber(long lexNumber) {
    	long number = 0;
       	int pos = pancakeLength;

       	while(--pos > 0) {
       		long c = lexNumber / factorials[pos];
       		number |= c << (pos * 4);
       		lexNumber -= factorials[pos] * c;
       	}
       	
       	int max = pancakeLength*4; 
    	for(int i=4; i<max; i+=4) {
       		// have to adjust with adjacent numbers.
       		byte num2 = (byte) ((number >> i) & 0x0F);
       		for(int x=i-4; x>=0; x-=4) {
   				byte num3 = (byte) ((number >> x) & 0x0F);
   				if(num3 >= num2) {
   					number += ((long)1) << x;
   				}
   			}
       	}
       	
       	return number;
    }

    void initExecutorService(int chunks) {
    	int threads = Math.min(THREADS, chunks);
    	executorService = Executors.newFixedThreadPool(threads); 

    	callables = new ArrayList<MyWorker>();

    	for(int i=0; i<threads; i++) {
			callables.add(new MyWorker());
		}
	}

    AtomicInteger chunksToExpand = new AtomicInteger();
    
	class MyWorker implements Callable<MyWorker> {
		ByteArray64 buffer;

		long currentLevelCounter;
		long endLexNumber;
		RandomAccessFile[] childNodeFilesNext;
		RandomAccessFile[] childNodeFilesCurrent;
		int id;
	    ByteBuffer[] tmpBuf;
		int currentChunk;

		public void reset() throws IOException {
			currentLevelCounter = 0;

			if(numberOfChunks > 1) {
				for(int chunk=0; chunk<numberOfChunks; chunk++) {
					childNodeFilesCurrent[chunk].setLength(0);
				}
				RandomAccessFile[] tmp = childNodeFilesNext;
				childNodeFilesNext = childNodeFilesCurrent;
				childNodeFilesCurrent = tmp;
			}
		}
		
		void init(int id, long bytesPerThread) {
			buffer = new ByteArray64(bytesPerThread);

			tmpBuf = new ByteBuffer[numberOfChunks];
    		for(int i=0; i<tmpBuf.length; i++) {
    			tmpBuf[i] = ByteBuffer.allocate(PAGE_SIZE);
    		}
    		if(numberOfChunks > 1) {
		    	try {
		    		childNodeFilesNext = new RandomAccessFile[numberOfChunks];
		    		childNodeFilesCurrent = new RandomAccessFile[numberOfChunks];
		    		for(int i=0; i<numberOfChunks; i++) {
		    			childNodeFilesNext[i] = new RandomAccessFile("T" + id + "L" + pancakeLength + "C" + i + "-NEXT", "rw");
		    			childNodeFilesCurrent[i] = new RandomAccessFile("T" + id + "L" + pancakeLength + "C" + i + "-CURRENT", "rw");
		    		}
		    	} catch (FileNotFoundException e) {
		    		e.printStackTrace();
		    	}
    		}
	    	this.id = id;
		}

		void mergeFiles() {
			if(numberOfChunks == 1) {
				return;
			}
			for(int i=0; i<callables.size(); i++) {
				mergeFile(callables.get(i).childNodeFilesCurrent[currentChunk], CURRENT_LEVEL);
			}
		}
		
		void mergeFile(RandomAccessFile file, byte level) {
			try {
				file.seek(0);
				FileChannel channel = file.getChannel();
				ByteBuffer buf = tmpBuf[0];
				buf.clear();
   				while(channel.read(buf)  > 0) {
   					buf.flip();
 
                    while (buf.remaining() > 4)
                    {
                    	byte b1 = buf.get();
                    	byte b2 = buf.get();
                    	byte b3 = buf.get();
                    	byte b4 = buf.get();
                     	byte b5 = buf.get();

                    	long lower = 0xFFFFFFFFL & (b2 & 0xFF) << 24 | (b3 & 0xFF) << 16 | (b4 & 0xFF) << 8 | (b5 & 0xFF) << 0;
            			long lexicalNumber =  ((long)b1 << 32) + lower;
            			                   			
                    	// Since we merge both CURRENT and NEXT files it is important that 
                    	// CURRENT have higher precedence than NEXT. 
    		   			boolean wasSet = buffer.setIfNotVisited(lexicalNumber, level);
    		   			if(DEBUG) {
    		   				long z = fromLexicalNumber(buffer.size * currentChunk * 4 + lexicalNumber);
    		   				System.out.println((wasSet ? "Merge " : "Not merged ") + (buffer.size * currentChunk * 4 + lexicalNumber) + " (" + Long.toHexString(z) + ") " + " chunk " + currentChunk);
    		   			}
                    }
                    buf.compact();
   				}
		   	} catch (Exception e) {
	    		e.printStackTrace();
		   	} finally {
		   		try {
					file.setLength(0);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	    	}
		}

		@Override
		public MyWorker call()  {
			try {				
				while((currentChunk = chunksToExpand.getAndDecrement()) >= 0) {
					endLexNumber = loadChunk(currentChunk, true);
					if(DEBUG) {
						System.out.println("THREAD " + id + ": " + currentChunk + " has " + (endLexNumber + 1) + " nodes.");
					}
					
					mergeFiles();

					bfs();

					storeChunk(currentChunk);
				}
			} catch(Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
			return this;
		}

		void bfs() throws IOException {
			long sourcePosition;
			long baseLexicalNumber = currentChunk * buffer.size * 4;     		

			for(int i=0; i<tmpBuf.length; i++) {
    			tmpBuf[i].clear();
    		}

    		buffer.startIterating(endLexNumber); 
    		
    		while((sourcePosition = buffer.getNextLevel(CURRENT_LEVEL)) != -1) {
    			currentLevelCounter++;
    			long sourceKey = fromLexicalNumber(baseLexicalNumber + sourcePosition);
    			
	    		// 
	    		// FOR EACH NODE ADD ALL POSSIBLE FLIPS TO GRAPH
	    		//
	    		for(int move=1; move<pancakeLength; move++) { // 0 is not a valid flip
	    			
	    			long destKey = flip(sourceKey, move, pancakeLength);
	    			long destLexicalNumber = toLexicalNumber(destKey);
	    			
    				storeDestination(destLexicalNumber, destKey);
	    		}
    		}
    		for(int i=0; i<tmpBuf.length; i++) {
				writeToFile(i);
    		}
		}
			
		/**
		 * Stores a node to memory if it fits, if not store the node to disk.
		 * 
		 */
		void storeDestination(long destLexicalNumber, long destKey) throws IOException {
			int c = (int) (destLexicalNumber / (buffer.size * 4));
			long lexNumber = destLexicalNumber - (long) (c * buffer.size * 4);

			boolean wasSet = true;
			if(currentChunk == c) {
				wasSet = buffer.setIfNotVisited(lexNumber, NEXT_LEVEL);
			} else {
				storeNodeToFile(c, lexNumber);
			}
			if (DEBUG) {
				System.out.println((wasSet ? "Store " : "Not store ") + ((currentChunk == c) ? "LOCAL " :"") + destLexicalNumber + " (" + Long.toHexString(destKey)+ ") " + " to chunk " + c);
			}
		}
		
		void storeNodeToFile(int c, long lexNumber) throws IOException {
			if(tmpBuf[c].remaining() < 5) {
				writeToFile(c);
			}
			tmpBuf[c].put((byte) ((lexNumber >>> 32) & 0xFF));
			tmpBuf[c].put((byte) ((lexNumber >>> 24) & 0xFF));
			tmpBuf[c].put((byte) ((lexNumber >>> 16) & 0xFF));
			tmpBuf[c].put((byte) ((lexNumber >>> 8) & 0xFF));
			tmpBuf[c].put((byte) ((lexNumber >>> 0) & 0xFF));
		}
		
		void writeToFile(int chunk) throws IOException {
			tmpBuf[chunk].flip();
			if(tmpBuf[chunk].hasRemaining()) {
				childNodeFilesNext[chunk].getChannel().write(tmpBuf[chunk]);
				tmpBuf[chunk].clear();
			}
		}
		

		/**
		 * Loads a chunk file.
		 * If setFinsihedLevel is true, any next child levels will be marked as processed so they are skipped in next round.
		 * 
		 * @param chunk
		 */
		private long loadChunk(int chunk, boolean setFinishedLevel) {
			if(numberOfChunks == 1) {
				if(setFinishedLevel) {
					buffer.setAll(NEXT_LEVEL, FINISHED_LEVEL);
				}
			} else {

			   	try {
			   		long i = 0;
			   		for(int idx=0; idx<buffer.data.length; idx++) {
				   		MappedByteBuffer buf = raf[chunk].getChannel().map(READ_WRITE, i, Math.min(raf[chunk].getChannel().size() - i, buffer.data[idx].length));
				   		i += buffer.data[idx].length;
				   		buf.get(buffer.data[idx]);
				   		clean(buf);
			   		}
			   		if(setFinishedLevel) {
			   			buffer.setAll(NEXT_LEVEL, FINISHED_LEVEL);
			   		}
		    	} catch (Exception e) {
		    		e.printStackTrace();
		    	}
			}
			
			long lastLexNumber;
			if(currentChunk == numberOfChunks - 1) {
				lastLexNumber = factorial(pancakeLength) - (numberOfChunks - 1) * buffer.size * 4 - 1;
			} else {
				lastLexNumber = buffer.size * 4 - 1;
			}
			return lastLexNumber;
		}	

		/**
		 * Store buffer data as 2 bit.
		 * @param chunk
		 */
		private void storeChunk(int chunk) {
			if(numberOfChunks == 1) {
				return; 
			}
			try {
				// STORE AS 2 bit.
				MappedByteBuffer buf = raf[chunk].getChannel().map(READ_WRITE, 0, Math.min(raf[chunk].getChannel().size(), MAX_VALUE));
				long i = 0;
				for (int j = 0; j < buffer.data.length; j++) {
					if (buf.remaining() < buffer.data[j].length) {
						buf.flip();
						clean(buf);
						buf = raf[chunk].getChannel().map(READ_WRITE, i, Math.min(raf[chunk].getChannel().size() - i, MAX_VALUE));
					}
					i += buffer.data[j].length;
					buf.put(buffer.data[j]);
				}
				buf.flip();
				clean(buf);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}	
		
	    public void printLastNodes() {
	    	System.out.println("LAST NODES:");
	    	
			for(int chunk=0; chunk<numberOfChunks; chunk++) {

				endLexNumber = loadChunk(chunk, false);
	    		buffer.startIterating(endLexNumber);
	    		
				long sourcePosition = 0;
	    		while((sourcePosition = buffer.getNextLevel(CURRENT_LEVEL)) != -1) {
	    			
					long key = fromLexicalNumber(chunk * buffer.size * 4 + sourcePosition);
					long reversedKey = flip(key, pancakeLength-1, pancakeLength);
					decodeLong(reversedKey, pancakes);
					
					System.out.println(Arrays.toString(pancakes));
				}
			}
	    }
	}
	
		
	/**
	* Creates all permutes of a stack of pancakes.
	* Breadth first search, two bit per stack from the first complete pancake stack
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	int bfs(long vertexes) throws InterruptedException, IOException 
    {
		// Get the lexicographic number for a complete pancake stack (N..543210) 
		// and write it to file as a starting point for the bfs. 
		try { 
			long lex = factorial(pancakeLength) - 1;
			int c = (int) (lex / (callables.get(0).buffer.size * 4));
			callables.get(0).storeDestination(lex, fromLexicalNumber(lex));
			callables.get(0).writeToFile(c);			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
    	long currentLevelCounter = 0;
    	long nodesLeft = factorial(pancakeLength);
    	int level = -1;

    	while(nodesLeft > 0) {

    		level++;
    		currentLevelCounter = 0;

    		// ZERO copy, just flip definitions.
    		byte tmpLevel = CURRENT_LEVEL;
    		CURRENT_LEVEL = NEXT_LEVEL;
    		NEXT_LEVEL = tmpLevel;
    		
    		chunksToExpand.set(numberOfChunks - 1);
    		for(int i =0; i<callables.size(); i++) {
    			callables.get(i).reset();
    		}

    		executorService.invokeAll(callables);
				
   			for(int i =0; i<callables.size(); i++) {
   				currentLevelCounter += callables.get(i).currentLevelCounter;
   			} 

    		System.out.println("LEVEL " + level + " NODES=" + currentLevelCounter);
    		nodesLeft -= currentLevelCounter;
    		if(currentLevelCounter == 0) {
    			throw new RuntimeException("NO NODES FOUND!");
    		}
    	}  
    	
    	callables.get(0).printLastNodes();

    	return level;
    }
} 