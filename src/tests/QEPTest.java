package tests;

// YOUR CODE FOR PART3 SHOULD GO HERE.

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.*;

import java.io.FileNotFoundException;
import java.util.Scanner;
import java.io.File;

public class QEPTest extends TestDriver {

    /** The display name of the test suite. */
    private static final String TEST_NAME = "QEPTest";

    private static Schema department;
    private static Schema employee;


    public static void main(String argv[]){
        QEPTest qepTest = new QEPTest();
        qepTest.create_minibase();

        // initialize schema for the "Drivers" table
        department = new Schema(4);
        department.initField(0, AttrType.INTEGER, 4, "DeptId");
        department.initField(1, AttrType.STRING, 20, "Name");
        department.initField(2, AttrType.INTEGER, 4, "MinSalary");
        department.initField(3, AttrType.INTEGER, 4, "MaxSalary");

        // initialize schema for the "Drivers" table
        employee = new Schema(5);
        employee.initField(0, AttrType.INTEGER, 4, "EmpId");
        employee.initField(1, AttrType.STRING, 20, "Name");
        employee.initField(2, AttrType.INTEGER, 4, "Age");
        employee.initField(3, AttrType.INTEGER, 4, "Salary");
        employee.initField(4, AttrType.INTEGER, 4, "DepId");

        System.out.println("\n" + "Running " + TEST_NAME + "...\n");
        boolean status = PASS;
        status &= qepTest.test1();
        status &= qepTest.test2();
        status &= qepTest.test3();
        status &= qepTest.test4();

        // display the final results
        System.out.println();
        if (status != PASS) {
            System.out.println("Error(s) encountered during " + TEST_NAME + ".");
        } else {
            System.out.println("All " + TEST_NAME
                    + " completed; verify output for correctness.");
        }

    }

    private boolean test1() {
        HeapFile hpEmp = new HeapFile(null);
        HashIndex indexEmp = new HashIndex(null);

        readEmployee(hpEmp, indexEmp);

        System.out.println("\n  ~> Test 1: Display for each employee his ID, Name and Age ...\n");
        FileScan scan = new FileScan(employee, hpEmp);
        Projection pro = new Projection(scan, 0, 1, 2);
        pro.execute();
        System.out.print("\n\nTest 1 completed without exception.\n");
        return PASS;
    }

    private boolean test2() {
        HeapFile hpDep = new HeapFile(null);
        HashIndex indexDep = new HashIndex(null);

        readDepartment(hpDep, indexDep);

        System.out.println("\n  ~> Test 2: Display the Name for the departments with MinSalary = MaxSalary ...\n");

        Predicate[] preds = new Predicate[] {
                new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3)};

        FileScan scan = new FileScan(department, hpDep);

        Selection sel = new Selection(scan, preds);
        sel.execute();
        System.out.print("\n\nTest 2 completed without exception.\n");


        return PASS;
    }

    private boolean test3() {
        HeapFile hpEmp = new HeapFile(null);
        HashIndex indexEmp = new HashIndex(null);
        HeapFile hpDep = new HeapFile(null);
        HashIndex indexDep = new HashIndex(null);

        readEmployee(hpEmp, indexEmp);
        readDepartment(hpDep, indexDep);

        System.out.println("\n  ~> Test 3: For each employee, display his Name and the Name of his department as well as the\n" +
                "maximum salary of his department ...\n");

        FileScan scanEmp = new FileScan(employee, hpEmp);
        FileScan scanDep = new FileScan(department, hpDep);

        Predicate[] preds = new Predicate[] {
                new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5)};

        HashJoin sj = new HashJoin(scanEmp, scanDep, preds);
        Projection pro = new Projection(sj, 1, 6, 8);
        pro.execute();
        System.out.print("\n\nTest 3 completed without exception.\n");

        return PASS;
    }

    private boolean test4() {
        HeapFile hpEmp = new HeapFile(null);
        HashIndex indexEmp = new HashIndex(null);
        HeapFile hpDep = new HeapFile(null);
        HashIndex indexDep = new HashIndex(null);

        readEmployee(hpEmp, indexEmp);
        readDepartment(hpDep, indexDep);

        System.out.println("\n  ~> Test 4: Display the Name for each employee whose Salary is greater than the maximum salary\n" +
                "of his department ...\n");

        FileScan scanEmp = new FileScan(employee, hpEmp);
        FileScan scanDep = new FileScan(department, hpDep);

        Predicate[] predsJoin = new Predicate[] {
                new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 4, AttrType.FIELDNO, 5)};

        Predicate[] predsSel = new Predicate[] {
                new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8)};

        HashJoin sj = new HashJoin(scanEmp, scanDep, predsJoin);
        Selection sel = new Selection(sj, predsSel);
        Projection pro = new Projection(sel, 1);
        pro.execute();
        System.out.print("\n\nTest 4 completed without exception.\n");

        return PASS;
    }

    private void readDepartment(HeapFile hpDep, HashIndex indexDep){
        Tuple t = new Tuple(department);

        try {
            Scanner scanner = new Scanner(new File("src/tests/SampleData/Department.txt"));
            scanner.nextLine();
            while(scanner.hasNext()){
                String[] str = scanner.nextLine().split(", ");
                t.setIntFld(0, Integer.parseInt(str[0]));
                t.setStringFld(1, str[1]);
                t.setIntFld(2, Integer.parseInt(str[2]));
                t.setIntFld(3, Integer.parseInt(str[3]));

                // insert the tuple in the file and index
                RID rid = hpDep.insertRecord(t.getData());
                indexDep.insertEntry(new SearchKey(Integer.parseInt(str[0])), rid);
            }
            scanner.close();

            System.out.println();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void readEmployee(HeapFile hpEmp, HashIndex indexEmp){
        Tuple t = new Tuple(employee);
        employee.print();

        try {
            Scanner scanner = new Scanner(new File("src/tests/SampleData/Employee.txt"));
            scanner.nextLine();
            while(scanner.hasNext()){
                String[] str = scanner.nextLine().split(", ");
                t.setIntFld(0, Integer.parseInt(str[0]));
                t.setStringFld(1, str[1]);
                t.setIntFld(2, Integer.parseInt(str[2]));
                t.setIntFld(3, Integer.parseInt(str[3]));
                t.setIntFld(4, Integer.parseInt(str[4]));
                t.print();

                // insert the tuple in the file and index
                RID rid = hpEmp.insertRecord(t.getData());
                indexEmp.insertEntry(new SearchKey(Integer.parseInt(str[4])), rid);
            }
            scanner.close();
            System.out.println();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
