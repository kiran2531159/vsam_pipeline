      *================================================================*
      * CUSTOMER MASTER RECORD - FOR MOSTLYAI TRAINING
      * Key: CUST-ID
      * Maps 1:1 to sample_data/customers.csv columns
      *================================================================*
       01  CUSTOMER-RECORD.
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
