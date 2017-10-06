import java.io.PrintStream;
import java.util.*;

public class MVTOTest {
	// This file contains some test examples. Create more tests of your own to debug your system. 
	private static PrintStream log = System.out;

	public static void main(String[] args) {
		
		// # of test to execute
		int TEST = 1;
		
		// For automatic validation, it is not possible to execute all tests at once
		// You can get the TEST# from args and execute all tests using a shell-script
		if(args.length > 0) {
			TEST = Integer.parseInt(args[0]);
		}
		
		try {
			switch (TEST) {
				case 1: test1(); break;
				case 2: test2(); break;
				case 3: test3(); break;
				case 4: test4(); break;
				case 5: test5(); break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static void test1() {
		log.println("----------- Test 1 -----------");
		/* Example schedule:
		 T1: I(1) C
		 T2:        R(1) W(1)           R(1) W(1) C
		 T3:                  R(1) W(1)             C
		*/
		int[][][] schedule = new int[][][]{
			/*T1:*/ {I(1),__C_                                        },
			/*T2:*/ {____,____,R(1),W(1),____,____,R(1),W(1),__C_     },
			/*T3:*/ {____,____,____,____,R(1),W(1),____,____,____,__C_}
		};
//		T(1):I(1,4)
//		T(1):COMMIT START
//		T(1):COMMIT FINISH
//		T(2):R(1) => 4
//		T(2):W(1,10)
//		T(3):R(1) => 10
//		T(3):W(1,14)
//		T(2):R(1) => 10
//		T(2):W(1,18)
//		T(2):ROLLBACK
//		T(3):ROLLBACK
//		    ROLLBACK T(2):W(1,18)
//		T(3):COMMIT START
//		    T(3) DOES NOT EXIST
		int maxLen = analyzeSchedule(schedule);
		printSchedule(schedule);
		Object[][] expectedResults = new Object[schedule.length][maxLen];
		expectedResults[T(2)][STEP(3)] = STEP(1);
		expectedResults[T(3)][STEP(5)] = STEP(4);
		expectedResults[T(2)][STEP(7)] = STEP(4);
		executeSchedule(schedule, expectedResults, maxLen);
	}
		
	private static void test2() {
		log.println("----------- Test 2 -----------");
		/* Example schedule:
		 T1: I(3) C
		 T2:        R(3)          R(3) C
		 T3:             W(3)               C
		*/
		int[][][] schedule = new int[][][]{
			/*T1:*/ {I(3),__C_                         },
			/*T2:*/ {____,____,R(3),____,R(3),__C_     },
			/*T3:*/ {____,____,____,W(3),____,____,__C_}
		};
//		T(1):I(3,4)
//		T(1):COMMIT START
//		T(1):COMMIT FINISH
//		T(2):R(3) => 4
//		T(3):W(3,10)
//		T(2):R(3) => 4
//		T(2):COMMIT START
//		T(2):COMMIT FINISH
//		T(3):COMMIT START
//		T(3):COMMIT FINISH
		int maxLen = analyzeSchedule(schedule);
		printSchedule(schedule);
		Object[][] expectedResults = new Object[schedule.length][maxLen];
		expectedResults[T(2)][STEP(3)] = STEP(1);
		expectedResults[T(2)][STEP(5)] = STEP(1);
		executeSchedule(schedule, expectedResults, maxLen);
	}

	private static void test3() {
		log.println("----------- Test 3 -----------");
		/* Example schedule:
		 T1: I(4) C
		 T2:        R(4) W(4)           R(4) W(4) C
		 T3:                  R(4) W(4)             C
		*/
		int[][][] schedule = new int[][][]{
			/*T1:*/ {I(4),__C_                                        },
			/*T2:*/ {____,____,R(4),W(4),____,____,____,R(4),W(4),__C_},
			/*T3:*/ {____,____,____,____,R(4),W(4),__C_               }
		};
//		T(1):I(4,4)
//		T(1):COMMIT START
//		T(1):COMMIT FINISH
//		T(2):R(4) => 4
//		T(2):W(4,10)
//		T(3):R(4) => 10
//		T(3):W(4,14)
//		T(3):COMMIT START
//		T(2):R(4) => 10
//		T(2):W(4,20)
//		T(2):ROLLBACK
//		T(3):COMMIT UNSUCCESSFUL
//		T(3):ROLLBACK
//		    ROLLBACK T(2):W(4,20)
		int maxLen = analyzeSchedule(schedule);
		printSchedule(schedule);
		Object[][] expectedResults = new Object[schedule.length][maxLen];
		expectedResults[T(2)][STEP(3)] = STEP(1);
		expectedResults[T(3)][STEP(5)] = STEP(4);
		expectedResults[T(2)][STEP(8)] = STEP(4);
		executeSchedule(schedule, expectedResults, maxLen);
	}
	
	private static void test4() {
		log.println("----------- Test 4 -----------");
		/* Example schedule:
		 T1: I(6) C
		 T2:        R(6)          W(6) C
		 T3:             R(6)   C
		*/
		int[][][] schedule = new int[][][]{
			/*T1:*/ {I(6),__C_                         },
			/*T2:*/ {____,____,R(6),____,____,W(6),__C_},
			/*T3:*/ {____,____,____,R(6),__C_          }
		};
//		T(1):I(6,4)
//		T(1):COMMIT START
//		T(1):COMMIT FINISH
//		T(2):R(6) => 4
//		T(3):R(6) => 4
//		T(3):COMMIT START
//		T(3):COMMIT FINISH
//		T(2):W(6,14)
//		T(2):ROLLBACK
//		    ROLLBACK T(2):W(6,14)
		int maxLen = analyzeSchedule(schedule);
		printSchedule(schedule);
		Object[][] expectedResults = new Object[schedule.length][maxLen];
		expectedResults[T(2)][STEP(3)] = STEP(1);
		expectedResults[T(3)][STEP(4)] = STEP(1);
		executeSchedule(schedule, expectedResults, maxLen);
	}

	private static void test5() {
		log.println("----------- Test 5 -----------");
		/* Example schedule:
		 T1: I(2),     I(7),                                C 
		 T2:      I(2),     W(2),               R(2),  C 
		 T3:                     R(2),R(7),  C 
		*/
		int[][][] schedule = new int[][][]{
			/*T1:*/ {I(2),____,I(7),____,____,____,____,____,____,__C_},
			/*T2:*/ {____,I(2),____,W(2),____,____,____,R(2),__C_     },
			/*T3:*/ {____,____,____,____,R(2),R(7),__C_               }
		};
//		T(1):I(2,4)
//		T(2):I(2,6)
//		T(2):ROLLBACK
//		    KEY ALREADY EXISTS IN T(2):I(2)
//		T(1):I(7,8)
//		T(3):R(2) => 4
//		T(3):R(7) => 8
//		T(3):COMMIT START
//		T(1):COMMIT START
//		T(1):COMMIT FINISH
//		T(3):COMMIT FINISH
		int maxLen = analyzeSchedule(schedule);
		printSchedule(schedule);
		Object[][] expectedResults = new Object[schedule.length][maxLen];
		expectedResults[T(3)][STEP(5)] = STEP(1);
		expectedResults[T(3)][STEP(6)] = STEP(3);
		executeSchedule(schedule, expectedResults, maxLen);
	}
	
	/**
	 * This method is for executing a schedule.
	 * 
	 * @param schedule is a 3D array containing one transaction 
	 *                 in each row, and in each cell is one operation
	 * @param expectedResults is the array of expected result in each
	 *                 READ operation. The cell contains the STEP# 
	 *                 in the schedule that WROTE or INSERTED 
	 *				   the value that should be read here.
	 * @param maxLen is the maximum length of schedule
	 */
	private static void executeSchedule(int[][][] schedule, Object[][] expectedResults, int maxLen) {
		Map<Integer, Integer> xactLabelToXact = new HashMap<Integer, Integer>();
		Set<Integer> ignoredXactLabels = new HashSet<Integer>();

		for(int step=0; step<maxLen; step++) {
			for(int i=0; i<schedule.length; i++) {
				if(step < schedule[i].length && schedule[i][step] != null) {
					int[] xactOps = schedule[i][step];
					int xactLabel = i+1;
					if(ignoredXactLabels.contains(xactLabel)) break;
					
					int xact = 0;
					try {
						if(xactLabelToXact.containsKey(xactLabel)) {
							xact = xactLabelToXact.get(xactLabel);
						} else {
							xact = MVTO.begin_transaction();
							xactLabelToXact.put(xactLabel, xact);
						}
						if(xactOps.length == 1) {
							switch(xactOps[0]) {
								case COMMIT: MVTO.commit(xact); break;
								case ROLL_BACK: MVTO.rollback(xact); break;
							}
						} else {
							switch(xactOps[0]) {
								case INSERT: MVTO.insert(xact, xactOps[1], getValue(step)); break;
								case WRITE: MVTO.write(xact, xactOps[1], getValue(step)); break;
								case READ: {
									int readValue = MVTO.read(xact, xactOps[1]);
									int expected = getValue((Integer)expectedResults[T(xactLabel)][step]);
									if(readValue != expected) {
										throw new WrongResultException(xactLabel, step, xactOps, readValue, expected);
									}
									break;
								}
							}
						}
					} catch (WrongResultException e) {
						throw e;
					} catch (Exception e) {
						ignoredXactLabels.add(xactLabel);
						log.println("    "+e.getMessage());
						//e.printStackTrace();
					}
					break;
				}
			}
		}
	}
	/**
	 * @param step is the STEP# in the schedule (zero-based)
	 * @return the expected result of a READ operation in a schedule.
	 */
	private static int getValue(int step) {
		return (step+2)*2;
	}

	private static void printSchedule(int[][][] schedule) {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<schedule.length; i++) {
			sb.append("T").append(i+1).append(": ");
			for(int j=0; j<schedule[i].length; j++) {
				int[] xactOps = schedule[i][j];
				if(xactOps == null) {
					sb.append("     ");
				} else if(xactOps.length == 1) {
					switch(xactOps[0]) {
						case COMMIT: sb.append("  C "); break;
						case ROLL_BACK: sb.append(" RB "); break;
					}
				} else {
					switch(xactOps[0]) {
						case INSERT: sb.append("I"); break;
						case WRITE: sb.append("W"); break;
						case READ: sb.append("R"); break;
					}
					sb.append("(").append(xactOps[1]).append(")");
				}
				if(j+1<schedule[i].length && xactOps != null){
					sb.append(",");
				}
			}
			sb.append("\n");
		}
		log.println("\n"+sb.toString());
	}

	/**
	 * Analyzes and validates the given schedule.
	 * 
	 * @return maximum number of steps in the
	 *         transactions inside the given schedule
	 */
	private static int analyzeSchedule(int[][][] schedule) {
		int maxLen = 0;
		for(int i=0; i<schedule.length; i++) {
			if(maxLen < schedule[i].length) {
				maxLen = schedule[i].length;
			}
			for(int j=0; j<schedule[i].length; j++) {
				int[] xactOps = schedule[i][j];
				if(xactOps == null) {
					// no operation
				} else if(xactOps.length == 1 && (xactOps[0] == COMMIT || xactOps[0] == ROLL_BACK)) {
					// commit or roll back
				} else if(xactOps.length == 2){
					switch(xactOps[0]) {
						case INSERT: /*insert*/ break;
						case WRITE: /*write*/; break;
						case READ: /*read*/; break;
						default: throw new RuntimeException("Unknown operation in schedule: T"+(i+1)+", Operation "+(j+1));
					}
				} else {
					throw new RuntimeException("Unknown operation in schedule: T"+(i+1)+", Operation "+(j+1));
				}
			}
		}
		return maxLen;
	}
	
	private final static int INSERT = 1, WRITE = 2, READ = 3, COMMIT = 4, ROLL_BACK = 5;
	private final static int[] __C_ = {COMMIT}, _RB_ = {ROLL_BACK}, ____ = null;

	//transaction
	private static int T(int i) {
		return i-1;
	}
	//step
	private static int STEP(int i) {
		return i-1;
	}
	//insert
	public static int[] I(int key) {
		return new int[]{INSERT,key};
	}
	//write
	public static int[] W(int key) {
		return new int[]{WRITE,key};
	}
	//read
	public static int[] R(int key) {
		return new int[]{READ,key};
	}
}

class WrongResultException extends RuntimeException {
	private static final long serialVersionUID = -7630223385777784923L;

	public WrongResultException(int xactLabel, int step, int[] operation, Object actual, Object expected) {
		super("Wrong result in T("+xactLabel+") in step " + (step+1) + " (Actual: " + actual+", Expected: " + expected + ")");
	}
}
