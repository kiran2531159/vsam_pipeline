      *================================================================*
      * CUSTOMER MASTER RECORD - VSAM KSDS FILE
      * Key: CUST-ID
      *================================================================*
       01  CUSTOMER-RECORD.
           05  CUST-ID                    PIC 9(10).
           05  CUST-PERSONAL-INFO.
               10  CUST-FIRST-NAME        PIC X(25).
               10  CUST-LAST-NAME         PIC X(30).
               10  CUST-MIDDLE-INIT       PIC X(01).
               10  CUST-DOB               PIC 9(08).
               10  CUST-GENDER            PIC X(01).
               10  CUST-SSN               PIC 9(09).
           05  CUST-ADDRESS-INFO.
               10  CUST-ADDR-LINE-1       PIC X(35).
               10  CUST-ADDR-LINE-2       PIC X(35).
               10  CUST-CITY              PIC X(25).
               10  CUST-STATE             PIC X(02).
               10  CUST-ZIP-CODE          PIC 9(05).
               10  CUST-COUNTRY           PIC X(03).
           05  CUST-CONTACT-INFO.
               10  CUST-PHONE             PIC 9(10).
               10  CUST-EMAIL             PIC X(50).
           05  CUST-ACCOUNT-INFO.
               10  CUST-ACCT-TYPE         PIC X(02).
               10  CUST-STATUS            PIC X(01).
               10  CUST-OPEN-DATE         PIC 9(08).
               10  CUST-CREDIT-LIMIT      PIC S9(09)V99 COMP-3.
           05  FILLER                     PIC X(20).
