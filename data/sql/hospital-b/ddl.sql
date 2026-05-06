CREATE SCHEMA IF NOT EXISTS public;
SET search_path TO public;

-- 1. departments
CREATE TABLE departments (
    deptid varchar(50) NOT NULL,
    name   varchar(50) NOT NULL,
    CONSTRAINT pk_departments PRIMARY KEY (deptid)
);

-- 2. encounters
CREATE TABLE encounters (
    encounterid    varchar(50) NOT NULL,
    patientid      varchar(50) NOT NULL,
    encounterdate  date        NOT NULL,
    encountertype  varchar(50) NOT NULL,
    providerid     varchar(50) NOT NULL,
    departmentid   varchar(50) NOT NULL,
    procedurecode  integer     NOT NULL,
    inserteddate   date        NOT NULL,
    modifieddate   date        NOT NULL,
    CONSTRAINT pk_encounters PRIMARY KEY (encounterid)
);

-- 3. patients (Hospital2_Patient_Data)
CREATE TABLE patients (
    id           varchar(50)  NOT NULL,
    f_name       varchar(50)  NOT NULL,
    l_name       varchar(50)  NOT NULL,
    m_name       varchar(50)  NOT NULL,
    ssn          varchar(50)  NOT NULL,
    phonenumber  varchar(50)  NOT NULL,
    gender       varchar(50)  NOT NULL,
    dob          date         NOT NULL,
    address      varchar(100) NOT NULL,
    modifieddate date         NOT NULL,
    CONSTRAINT pk_patients PRIMARY KEY (id)
);

-- 4. providers
CREATE TABLE providers (
    providerid      varchar(50) NOT NULL,
    firstname       varchar(50) NOT NULL,
    lastname        varchar(50) NOT NULL,
    specialization  varchar(50) NOT NULL,
    deptid          varchar(50) NOT NULL,
    npi             bigint      NOT NULL,
    CONSTRAINT pk_providers PRIMARY KEY (providerid)
);

-- 5. transactions
CREATE TABLE transactions (
    transactionid   varchar(50) NOT NULL,
    encounterid     varchar(50) NOT NULL,
    patientid       varchar(50) NOT NULL,
    providerid      varchar(50) NOT NULL,
    deptid          varchar(50) NOT NULL,
    visitdate       date        NOT NULL,
    servicedate     date        NOT NULL,
    paiddate        date        NOT NULL,
    visittype       varchar(50) NOT NULL,
    amount          double precision NOT NULL,
    amounttype      varchar(50) NOT NULL,
    paidamount      double precision NOT NULL,
    claimid         varchar(50) NOT NULL,
    payorid         varchar(50) NOT NULL,
    procedurecode   integer     NOT NULL,
    icdcode         varchar(50) NOT NULL,
    lineofbusiness  varchar(50) NOT NULL,
    medicaidid      varchar(50) NOT NULL,
    medicareid      varchar(50) NOT NULL,
    insertdate      date        NOT NULL,
    modifieddate    date        NOT NULL,
    CONSTRAINT pk_transactions PRIMARY KEY (transactionid)
);