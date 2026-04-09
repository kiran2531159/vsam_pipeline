      *================================================================*
      * ACCOUNT RECORD - VSAM KSDS FILE
      * Key: ACCT-NUMBER
      * Foreign Key: ACCT-CUST-ID -> CUSTOMER.CUST-ID
      *================================================================*
       01  ACCOUNT-RECORD.
           05  ACCT-NUMBER                PIC 9(12).
           05  ACCT-CUST-ID              PIC 9(10).
           05  ACCT-TYPE                  PIC X(03).
           05  ACCT-STATUS                PIC X(01).
           05  ACCT-OPEN-DATE             PIC 9(08).
           05  ACCT-BALANCE               PIC S9(11)V99.
           05  ACCT-INTEREST-RATE         PIC 9(03)V9(04).
           05  ACCT-CREDIT-LIMIT          PIC S9(11)V99.
           05  ACCT-LAST-ACTIVITY-DATE    PIC 9(08).
           05  ACCT-BRANCH-CODE           PIC X(05).
           05  ACCT-ROUTING-NUM           PIC 9(09).
           05  FILLER                     PIC X(10).
