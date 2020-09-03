CREATE TABLE IF NOT EXISTS health_table (
     id serial CONSTRAINT health_table_pk PRIMARY KEY,
     registered_doctor TEXT,
     has_asthma BOOL,
     has_registered_disability BOOL,
     record_created_at TIMESTAMP DEFAULT now(),
     record_updated_at TIMESTAMP DEFAULT now()
 );

 CREATE TABLE IF NOT EXISTS id_register (
      id serial CONSTRAINT id_register_pk PRIMARY KEY,
      name TEXT,
      record_created_at TIMESTAMP DEFAULT now(),
      record_updated_at TIMESTAMP DEFAULT now()
  );
