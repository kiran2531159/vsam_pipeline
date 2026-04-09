      *================================================================*
      * TRANSACTION RECORD - VSAM ESDS FILE
      * Foreign Key: TXN-ACCT-NO -> ACCOUNT.ACCT-NUMBER
      *================================================================*
       01  TRANSACTION-RECORD.
           05  TXN-ID                     PIC 9(15).
           05  TXN-ACCT-NO               PIC 9(12).
           05  TXN-DATE                   PIC 9(08).
           05  TXN-TIME                   PIC 9(06).
           05  TXN-TYPE                   PIC X(02).
           05  TXN-AMOUNT                 PIC S9(09)V99.
           05  TXN-DESC                   PIC X(40).
           05  TXN-STATUS                 PIC X(01).
           05  TXN-MERCHANT-NAME          PIC X(30).
           05  TXN-MERCHANT-CITY          PIC X(20).
           05  TXN-MERCHANT-STATE         PIC X(02).
           05  TXN-AUTH-CODE              PIC X(06).
           05  TXN-POST-DATE              PIC 9(08).
           05  FILLER                     PIC X(10).
