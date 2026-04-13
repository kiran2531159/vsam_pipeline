      *================================================================*
      * ACCOUNT RECORD - FOR MOSTLYAI TRAINING
      * Key: ACCT-NUMBER  FK: ACCT-CUST-ID -> CUSTOMER.CUST-ID
      * Maps 1:1 to sample_data/accounts.csv columns
      *================================================================*
       01  ACCOUNT-RECORD.
           05  ACCT-NUMBER                PIC 9(10).
           05  ACCT-CUST-ID              PIC 9(10).
           05  ACCT-TYPE                  PIC X(02).
           05  ACCT-OPEN-DATE             PIC 9(08).
           05  ACCT-BALANCE               PIC 9(09).
           05  ACCT-CREDIT-LIMIT          PIC 9(09).
           05  ACCT-INT-RATE              PIC 9(05).
           05  ACCT-STATUS                PIC X(01).
           05  ACCT-BRANCH-ID             PIC X(05).
