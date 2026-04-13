      *================================================================*
      * COMBINED MULTI-TABLE RECORD LAYOUT
      * Merges: CUSTOMER + ACCOUNT + TRANSACTION into a single file.
      *
      * REC-TYPE identifies the record kind:
      *   "CU" = Customer,  "AC" = Account,  "TX" = Transaction
      *
      * Each record is padded to the same fixed length so that the
      * VSAM file can be read sequentially with a single record size.
      *
      * Relationship order in file:
      *   CU (customer)
      *     AC (account for that customer)
      *       TX (transaction for that account)
      *     AC ...
      *       TX ...
      *   CU ...
      *================================================================*
       01  COMBINED-RECORD.
           05  REC-TYPE                   PIC X(02).
      *    ── Customer fields (when REC-TYPE = "CU") ───────────────
           05  CUST-ID                    PIC 9(10).
           05  CUST-FIRST-NAME            PIC X(25).
           05  CUST-LAST-NAME             PIC X(30).
           05  CUST-DOB                   PIC 9(08).
           05  CUST-SSN                   PIC 9(09).
           05  CUST-ADDR-LINE1            PIC X(35).
           05  CUST-CITY                  PIC X(25).
           05  CUST-STATE                 PIC X(02).
           05  CUST-ZIP-CODE              PIC 9(05).
           05  CUST-PHONE                 PIC 9(10).
           05  CUST-EMAIL                 PIC X(50).
           05  CUST-STATUS                PIC X(01).
           05  CUST-OPEN-DATE             PIC 9(08).
           05  CUST-CREDIT-SCORE          PIC 9(03).
      *    ── Account fields (when REC-TYPE = "AC") ────────────────
           05  ACCT-NUMBER                PIC 9(10).
           05  ACCT-CUST-ID              PIC 9(10).
           05  ACCT-TYPE                  PIC X(02).
           05  ACCT-OPEN-DATE             PIC 9(08).
           05  ACCT-BALANCE               PIC 9(09).
           05  ACCT-CREDIT-LIMIT          PIC 9(09).
           05  ACCT-INT-RATE              PIC 9(05).
           05  ACCT-STATUS                PIC X(01).
           05  ACCT-BRANCH-ID             PIC X(05).
      *    ── Transaction fields (when REC-TYPE = "TX") ────────────
           05  TXN-ID                     PIC 9(10).
           05  TXN-ACCT-NO               PIC 9(10).
           05  TXN-DATE                   PIC 9(08).
           05  TXN-TIME                   PIC 9(06).
           05  TXN-TYPE                   PIC X(02).
           05  TXN-AMOUNT                 PIC 9(09).
           05  TXN-DESC                   PIC X(30).
           05  TXN-BALANCE-AFTER          PIC 9(09).
           05  TXN-CHANNEL                PIC X(03).
           05  TXN-STATUS                 PIC X(01).
