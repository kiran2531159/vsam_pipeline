      *================================================================*
      * EMPLOYEE RECORD - VSAM KSDS FILE
      * Key: EMP-ID
      * A standalone copybook example with more complex structures
      *================================================================*
       01  EMPLOYEE-RECORD.
           05  EMP-ID                     PIC 9(08).
           05  EMP-NAME.
               10  EMP-FIRST-NAME         PIC X(20).
               10  EMP-LAST-NAME          PIC X(25).
               10  EMP-MIDDLE-INIT        PIC X(01).
           05  EMP-TITLE                  PIC X(04).
           05  EMP-DOB                    PIC 9(08).
           05  EMP-GENDER                 PIC X(01).
           05  EMP-SSN                    PIC 9(09).
           05  EMP-ADDRESS.
               10  EMP-STREET             PIC X(30).
               10  EMP-CITY               PIC X(20).
               10  EMP-STATE              PIC X(02).
               10  EMP-ZIP                PIC 9(05).
           05  EMP-PHONE                  PIC 9(10).
           05  EMP-EMAIL                  PIC X(40).
           05  EMP-DEPT-CODE              PIC X(04).
           05  EMP-HIRE-DATE              PIC 9(08).
           05  EMP-SALARY                 PIC S9(07)V99 COMP-3.
           05  EMP-STATUS                 PIC X(01).
           05  FILLER                     PIC X(15).
