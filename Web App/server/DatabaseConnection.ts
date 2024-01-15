import { Pool } from 'pg'

const connectionString: string | undefined = process.env.POSTGRES_STRING;

export class DatabaseConnection {
    pool!: Pool;
  
    constructor() {
      try {
        this.pool = new Pool({connectionString});
      } catch (error) {
        console.log('Pool could not be created');
      }
    }
  
    connect = async () => {
      try {
        await this.pool.connect();
      } catch (error) {
        console.log('\nThere was an error connecting to the database');
        console.log(error);
      }
    }
  
    disconnect = async () => {
      await this.pool.end();
    }
  }