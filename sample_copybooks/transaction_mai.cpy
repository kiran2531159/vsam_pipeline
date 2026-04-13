      *================================================================*
      * TRANSACTION RECORD - FOR MOSTLYAI TRAINING
      * Key: TXN-ID  FK: TXN-ACCT-NO -> ACCOUNT.ACCT-NUMBER
      * Maps 1:1 to sample_data/transactions.csv columns
      *================================================================*
       01  TRANSACTION-RECORD.
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
