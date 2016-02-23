
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


/**
 * Exhaustive graph search program for the Pancake Sorting Problem
 * 
 * TBBFS, expanding a single chunk in memory using many threads.
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
 * will be responsible for read 2 GB. But they will write the child nodes over the full memory area.
 * Hence some sort of synchronization is needed. Here the two-bits are expanded in memory to a full byte.
 * This way we don't have to think about synchronizing any of the threads.
 * 
 * If the graph doesn't fit in memory,  disk storage will be used together with MemoryMappedFiles.
 * The graph itself is stored on disk named CHUNK-file (CHUNK0, CHUNK1... CHUNKN) with two-bit per state.   
 * Each thread will store its own NEXT/CURRENT files with the nodes to visit.
 * The nodes are stored as a 5-byte big endian encoded long.
 * 
 * 
 * The basic processing is simple: 
 * 1) Load a chunk to memory. Loads a two bit blob to memory and expand it to one byte.
 * 2) Merge all nodes from NEXT and CURRENT FILES. This is done by threads. 
 * 3) Breadth first search this chunk and store nodes to current chunk to NEXT/CURRENT files. Done by threads.
 * 4) Store chunk to disk as two bit per state.
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
 * Pancake 11: vertexes=39916800 chunks=1 threads=4 memory usage 38 MB
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
 * Time: 12994 millis. Used memory: 41,48 MB
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
class PancakesOneChunkManyThreads
{
    byte[] pancakes;
    int pancakeLength;
	long[] factorials;    
	int numberOfChunks;
 
	long BYTES_IN_MEMORY;
	long NUMBER_OF_BYTES_ALLOWED = Runtime.getRuntime().maxMemory();
	int THREADS = Runtime.getRuntime().availableProcessors();
	int PANCAKE_START = 1;
	int PANCAKE_END = 99;

	static final int PAGE_SIZE = 16*1024;
	static final boolean DEBUG = false;        

	ByteArray64 buffer;
    RandomAccessFile[] raf;

	ExecutorService executorService;
	
	public static byte NOT_VISITED    = 0;
	public static byte CURRENT_LEVEL  = 1; 
	public static byte NEXT_LEVEL     = 2;
	public static byte FINISHED_LEVEL = 3;
	
	List<MyWorker> callables;
	
	int chunk;	
	byte state;

	private static final byte MERGE_FILES = 0;
	private static final byte BFS = 1;
	
    public static void main (String args[])  
    {
    	try {
			new PancakesOneChunkManyThreads(args);
		} catch (OutOfMemoryError e) {
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

        public byte get( long index ) {
            int chunk = (int)(index/CHUNK_SIZE);
            int offset = (int)(index - (((long)chunk)*CHUNK_SIZE));
            return data[chunk][offset];   
        }
        
        public void set( long index, byte b ) {
            int chunk = (int)(index/CHUNK_SIZE);
            int offset = (int)(index - (((long)chunk)*CHUNK_SIZE));
            data[chunk][offset] = b;
        }

        public long size() {
            return size;
        }

		public void setAll(byte from, byte to) {
			for(int c=0; c<data.length; c++) {
				int offsets = data[c].length;
				for(int offset=0; offset<offsets; offset++) {
					if(data[c][offset] == from) {
						data[c][offset] = to;
					}
				}
			}
		}

		public boolean setIfNotVisitedOrNext(long index, byte bit) {
            int chunk = (int)(index/CHUNK_SIZE);
            int offset = (int)(index - (((long)chunk)*CHUNK_SIZE));
        	byte b = data[chunk][offset];   

        	if(b == NEXT_LEVEL || b == NOT_VISITED) {
	        	data[chunk][offset] = bit;
	        	return true;
        	}
        	return false;
		}

		public long getNextLevel(BufferIterator iterator, byte level) {
			while(true) {
				if(iterator.offset == CHUNK_SIZE) {
					iterator.chunk++;
					iterator.offset = 0;
				}				
				if(iterator.endChunk == iterator.chunk && iterator.offset == iterator.endOffset) {
					return -1;
				}
				if(data[iterator.chunk][iterator.offset++] == level) {
					return (long)iterator.chunk * CHUNK_SIZE + iterator.offset -1;
				}
			}
		}

        public void startIterating(BufferIterator iterator, long startIndex, long endIndex) {
			iterator.chunk = (int)(startIndex/CHUNK_SIZE);
			iterator.offset = (int)(startIndex - (((long)iterator.chunk)*CHUNK_SIZE));
	        		
			iterator.endChunk = (int)(endIndex/CHUNK_SIZE);
			iterator.endOffset = (int)(endIndex - (((long)iterator.endChunk)*CHUNK_SIZE));
		}
        
        public void startIterating(long startIndex, long endIndex) {
        	startIterating(iterator, startIndex, endIndex);
		}

		public void setNext(byte b) {
			data[iterator.chunk][iterator.offset++] = b;
			if(iterator.offset == CHUNK_SIZE) {
				iterator.chunk++;
				iterator.offset = 0;
			}
		}
		
		public byte getNext() {
			if(iterator.offset == CHUNK_SIZE) {
				iterator.chunk++;
				iterator.offset = 0;
			}
			byte b = data[iterator.chunk][iterator.offset++];
			
			return b;
		}
    }

    static class BufferIterator {
		int offset;
		int chunk;
        int endChunk;
        int endOffset;
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
				NUMBER_OF_BYTES_ALLOWED = Math.min(Long.parseLong(args[1]), NUMBER_OF_BYTES_ALLOWED);
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
    
    public PancakesOneChunkManyThreads(String[] args) {
/*    	 try {
			System.setOut(new PrintStream(new File("output-file.txt")));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
  */  	 
    	Runtime rt = Runtime.getRuntime();
    	System.out.println("Max mem:" + rt.maxMemory());
    	
    	long startTime;
    	setArgs(args);
    	initExecutorService();
    
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
	    	if(NUMBER_OF_BYTES_ALLOWED >= vertexes) {
	    		BYTES_IN_MEMORY = vertexes;
	    	} else if(vertexes % NUMBER_OF_BYTES_ALLOWED != 0){
	    		// must be a multiple of vertexes so we can calculate chunks.
	    		int chunks = (int)(vertexes / NUMBER_OF_BYTES_ALLOWED);
	    		BYTES_IN_MEMORY = (long)(vertexes / (chunks + 1));
	    	} else {
	    		BYTES_IN_MEMORY = NUMBER_OF_BYTES_ALLOWED;
	    	}
	    	
	    	if(BYTES_IN_MEMORY > 0xFFFFFFFFFFL) {
	    		throw new RuntimeException("Need to increase child size");
	    	}
	    	if(pancakeLength > 16) {
	    		throw new RuntimeException("Need to increase internal pancake format (long)");
	    	}
	    	
	    	numberOfChunks = (int) (vertexes / BYTES_IN_MEMORY);
    		try {
    			buffer = new ByteArray64(BYTES_IN_MEMORY);
    		} catch(OutOfMemoryError o) {
    			NUMBER_OF_BYTES_ALLOWED -= BYTES_IN_MEMORY / numberOfChunks;
    		}

	    	System.out.println("Pancake " + pancakeLength + ": vertexes=" + vertexes + " chunks=" + numberOfChunks + " threads=" + THREADS+ " memory usage " + (long)buffer.size / 1024 / 1024 +" MB");

	    	//
    		// Create chunk files
	    	//
    		removeOldFiles();
	    	 
    		raf = new RandomAccessFile[numberOfChunks];
    		if(numberOfChunks > 1) {
				for(int chunk=0; chunk<numberOfChunks; chunk++) {
	    			try {
	    				raf[chunk] = new RandomAccessFile("CHUNK" + chunk, "rw");
						if (chunk == numberOfChunks - 1) {
							raf[chunk].setLength(((vertexes - BYTES_IN_MEMORY * chunk) + 3)/ 4);
						} else {
							raf[chunk].setLength((BYTES_IN_MEMORY  + 3)/ 4);
						}
						raf[chunk].getFD().sync();
	    			} catch (FileNotFoundException e) {
	    				e.printStackTrace();
	    			} catch (IOException e) {
						e.printStackTrace();
					}
	    		}
			}
    		

    	   	//
	    	// SPLIT LEXICAL NUMBERS AMONG CPUS.
	    	//
	    	int cpus = callables.size();
	    	long start = 0;
	    	long chunk = buffer.size / cpus;
			for(int i=0; i<cpus; i++) {
				if(i == cpus-1) {
					callables.get(i).init(i, start, buffer.size);
				} else {
					callables.get(i).init(i, start, start+chunk);
				}
				start += chunk;
			}					

	    	//
	    	// Breadth first graph search.
	    	//
	    	try {
				bfs(vertexes);
			} catch (Exception e) {
				e.printStackTrace();
			} 
	    	
	    	printLastNodes();
			
	    	// Release memory mapped files.
	    	if(numberOfChunks > 1) {
				for (int c = 0; c < numberOfChunks; c++) {
					try {
			            raf[c].getChannel().close();
						raf[c].close();
						for(int t=0; t<THREADS; t++) {
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
    	}
    	
    	executorService.shutdown();
    }

    private void clean(MappedByteBuffer mappedBuffer) {
    	if (mappedBuffer instanceof sun.nio.ch.DirectBuffer) {
		    sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) mappedBuffer).cleaner();
		    cleaner.clean();
    	}	
    }
    
    private void printLastNodes() {
    	System.out.println("LAST NODES:");
    	
		for(chunk=0; chunk<numberOfChunks; chunk++) {
			loadChunk(chunk, false);
			long bytePosition = 0;
			while(bytePosition < buffer.size) {
				byte b = buffer.get(bytePosition);
				if(b == CURRENT_LEVEL) {
					long key = fromLexicalNumber(chunk * BYTES_IN_MEMORY + bytePosition);
					long reversedKey = flip(key, pancakeLength-1, pancakeLength);
					decodeLong(reversedKey, pancakes);
					System.out.println(Arrays.toString(pancakes));
				}
				bytePosition++;
			}
		}
    }
    
    private void removeOldFiles() {
    	File f; 
		for(int i=0; i<=numberOfChunks; i++) {
   			for(int id=0; id<callables.size(); id++) {   				
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
    long fromLexicalNumberNew(long lexNumber) {
       	int pos = pancakeLength;
       	byte[] b = pancakes;
       	int i = 0;
       	
       	while(--pos >= 0) {
       		b[i] = (byte) (lexNumber / factorials[pos]);
       		lexNumber %= factorials[pos];
       		i++;
       	}

       	for(i=pancakeLength-2; i>=0; i--) {
       		// have to adjust with adjacent numbers.
       		byte num = b[i];
   			for(int x=i+1; x<pancakeLength; x++) {
   				if(b[x] >= num) {
   					b[x]++;
   				}
   			}
       	}
       	
       	return encodeToLong(b);
    }   
    
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

    void initExecutorService() { 
    	executorService = Executors.newFixedThreadPool(THREADS); 

    	callables = new ArrayList<MyWorker>();

    	for(int i=0; i<THREADS; i++) {
			callables.add(new MyWorker());
		}
	}
	
	class MyWorker implements Callable<MyWorker> {
		long currentLevelCounter;
		long startLexNumber;
		long endLexNumber;
		BufferIterator iterator;
		RandomAccessFile[] childNodeFilesNext;
		RandomAccessFile[] childNodeFilesCurrent;
		int id;
	    ByteBuffer[] tmpBuf;

		public void reset() throws IOException {
			if(numberOfChunks > 1) {
				for(int chunk=0; chunk<numberOfChunks; chunk++) {
					childNodeFilesCurrent[chunk].setLength(0);
				}
				RandomAccessFile[] tmp = childNodeFilesNext;
				childNodeFilesNext = childNodeFilesCurrent;
				childNodeFilesCurrent = tmp;
			}
		}
		
		void init(int id, long startLexNumber, long endLexNumber) {
			iterator = new BufferIterator();
			tmpBuf = new ByteBuffer[numberOfChunks];
    		for(int i=0; i<tmpBuf.length; i++) {
    			tmpBuf[i] = ByteBuffer.allocate(PAGE_SIZE);
    		}
    		if(numberOfChunks > 1) {
		    	try {
		    		childNodeFilesNext = new RandomAccessFile[numberOfChunks];
		    		childNodeFilesCurrent = new RandomAccessFile[numberOfChunks];
		    		for(int i=0; i<numberOfChunks; i++) {
		    			childNodeFilesNext[i] = new RandomAccessFile("T" + id + "L" + pancakeLength + "C" + i+"-NEXT", "rw");
		    			childNodeFilesCurrent[i] = new RandomAccessFile("T" + id + "L" + pancakeLength + "C" + i + "-CURRENT", "rw");
		    		}
		    	} catch (FileNotFoundException e) {
		    		e.printStackTrace();
		    	}
    		}
	    	this.id = id;
			this.startLexNumber = startLexNumber;
			this.endLexNumber = endLexNumber;
		}

		void mergeFiles() {
			if(numberOfChunks == 1) {
				return;
			}
			mergeFile(childNodeFilesCurrent[chunk], CURRENT_LEVEL);
			mergeFile(childNodeFilesNext[chunk], NEXT_LEVEL);
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
    		   			boolean wasSet = buffer.setIfNotVisitedOrNext(lexicalNumber, level);
    		   			if(DEBUG) {
    		   				long z = fromLexicalNumber((BYTES_IN_MEMORY * chunk + lexicalNumber));
    		   				System.out.println((wasSet ? "Merge " : "Not merged ") + (BYTES_IN_MEMORY * chunk + lexicalNumber) + " (" + Long.toHexString(z)+ ") " + " chunk " + chunk);
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
				switch(state) {
				case MERGE_FILES:
					mergeFiles();
					break;
					
				case BFS:
					bfs();
					break;
				}
			} catch(Exception e) {
 				e.printStackTrace();
 				System.exit(-1);
			}
			return this;
		}
		
		void bfs() throws IOException {
			long sourcePosition;
			currentLevelCounter = 0;
    		for(int i=0; i<tmpBuf.length; i++) {
    			tmpBuf[i].clear();
    		}

    		buffer.startIterating(iterator, startLexNumber, endLexNumber);
    		long baseLexicalNumber = chunk * BYTES_IN_MEMORY;
    		
    		while((sourcePosition = buffer.getNextLevel(iterator, CURRENT_LEVEL)) != -1) {
    			currentLevelCounter++;
    			//long sourceKey = fromLexicalNumberNew(baseLexicalNumber + sourcePosition);  // THREAD SHARING pancake[]
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
			int c = (int) (destLexicalNumber / BYTES_IN_MEMORY);
			long lexNumber = destLexicalNumber - (long) c * BYTES_IN_MEMORY;

			boolean wasSet = true;
			if(chunk == c) {
				wasSet = buffer.setIfNotVisitedOrNext(lexNumber, NEXT_LEVEL);
			} else {
				storeNodeToFile(c, lexNumber);
			}
			if(DEBUG ) {
				System.out.println((wasSet ? "Store " : "Not store ") + destLexicalNumber + " (" + Long.toHexString(destKey)+ ") " + " to chunk " + c);
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
			chunk = 0;
			long lex = factorial(pancakeLength) - 1;
			int c = (int) (lex / BYTES_IN_MEMORY);
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
    		
			for (int i = 0; i < callables.size(); i++) {
				callables.get(i).reset();
			}
    		for(chunk=0; chunk<numberOfChunks; chunk++) {

    			loadChunk(chunk, true);
    			
				state = MERGE_FILES;
				executorService.invokeAll(callables);

				state = BFS;
				executorService.invokeAll(callables);
			
    			for(int i =0; i<callables.size(); i++) {
    				currentLevelCounter += callables.get(i).currentLevelCounter;
    			} 

    			storeChunk(chunk);
    		}

    		System.out.println("LEVEL " + level + " NODES=" + currentLevelCounter);
    		nodesLeft -= currentLevelCounter;
    		if(currentLevelCounter == 0) {
    			throw new RuntimeException("NO NODES FOUND!");
    		}
    	}  
    	return level;
    }

	/**
	 * Loads a chunk file consisting of 2 bit data and store it as one byte in memory.
	 * If setFinsihedLevel is true, any next child levels will be marked of as processes so they are skipped in next round.
	 * 
	 * @param chunk
	 */
	private void loadChunk(int chunk, boolean setFinishedLevel) {

		if(numberOfChunks == 1) {
			if(setFinishedLevel) {
				buffer.setAll(NEXT_LEVEL, FINISHED_LEVEL);
			}			
			return;
		}
		
	   	try {
	   		MappedByteBuffer buf = raf[chunk].getChannel().map(READ_WRITE, 0, Math.min(raf[chunk].getChannel().size(), MAX_VALUE));
			buffer.startIterating(0, buffer.size);			
			long i = 0;
			while(i<BYTES_IN_MEMORY) {
				if (!buf.hasRemaining()) {
					clean(buf);
					buf = raf[chunk].getChannel().map(READ_WRITE, i, Math.min(raf[chunk].getChannel().size() - i, MAX_VALUE));
				}
   				byte b = buf.get();
   	   			byte b1 = (byte) ((b >> 0) & FINISHED_LEVEL);
   	   			byte b2 = (byte) ((b >> 2) & FINISHED_LEVEL);
   	   			byte b3 = (byte) ((b >> 4) & FINISHED_LEVEL);
   	   			byte b4 = (byte) ((b >> 6) & FINISHED_LEVEL);
   	   			if(setFinishedLevel) {
	   	   			if(b1 == NEXT_LEVEL) {
	   	   				b1 = FINISHED_LEVEL;
	   	   			}
	   	   			if(b2 == NEXT_LEVEL) {
	   	   				b2 = FINISHED_LEVEL;
	   	   			}
	   	   			if(b3 == NEXT_LEVEL) {
	   	   				b3 = FINISHED_LEVEL;
	   	   			}
	   	   			if(b4 == NEXT_LEVEL) {
	   	   				b4 = FINISHED_LEVEL;
	   	   			}
   	   			}
   	   			i++;
   	   			buffer.setNext(b1);
				if (i++ < buffer.size)
					buffer.setNext(b2);
				if (i++ < buffer.size)
					buffer.setNext(b3);
				if (i++ < buffer.size)
					buffer.setNext(b4);
			}
		
   			buf.clear();
   			clean(buf);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
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
			buffer.startIterating(0, buffer.size);
			long i = 0;
			while(i<BYTES_IN_MEMORY) {
				i++;
				byte b1 = buffer.getNext();
				byte b2 = 0;
				byte b3 = 0;
				byte b4 = 0;
				if (i++ < buffer.size)
					b2 = buffer.getNext();
				if (i++ < buffer.size)
					b3 = buffer.getNext();
				if (i++ < buffer.size)
					b4 = buffer.getNext();

				byte b = (byte) (b1 | b2 << 2 | b3 << 4 | b4 << 6);
				if (!buf.hasRemaining()) {
					clean(buf);
					buf = raf[chunk].getChannel().map(READ_WRITE, i, Math.min(raf[chunk].getChannel().size() - i, MAX_VALUE));
				}
				buf.put(b);
			}
		
			buf.flip();
			clean(buf);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
	}	
} 
