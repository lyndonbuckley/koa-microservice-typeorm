import {Microservice} from "koa-microservice";
import {Connection, createConnection, getConnection, getConnectionOptions} from "typeorm";
import {ConnectionOptions} from "typeorm/connection/ConnectionOptions";
import {MysqlConnectionOptions} from "typeorm/driver/mysql/MysqlConnectionOptions";
import {PostgresConnectionOptions} from "typeorm/driver/postgres/PostgresConnectionOptions";

export type MicroserviceORMConnectionOptions = MysqlConnectionOptions | PostgresConnectionOptions;

export interface MicroserviceORMConfig {
    type: "mysql" | "postgres"
    master: string,
    slaves?: string | string[],
    port?: number,
    database?: string,
    username?: string,
    password?: string
}

export interface MicroserviceORMOptions {
    connectionName?: string;
    connectionOptions?: ConnectionOptions;
    connectionRequired?: boolean
    unhealthyWithoutConnection?: boolean
    connectionAttemptInterval?: number;
    healthCheckQuery?: string
    healthCheckInterval?: number;
    config?: MicroserviceORMConfig;
}
export class MicroserviceORM {
    app?: Microservice;
    connection: Connection | null;
    config?: MicroserviceORMConfig

    connectionRequired: boolean = true;
    unhealthyWithoutConnection: boolean = true;
    connectionAttemptInterval: number = 15000;
    healthCheckQuery: string = 'SELECT 1';
    healthCheckInterval: number = 15000;

    private _lastConnectionAttempt: Date | null = null;
    private _lastHealthCheck: Date | null = null;
    private _lastHealthCheckResult: boolean | null = null;

    private _connectionOptions?: ConnectionOptions;
    private _connectionName?: string;

    constructor(app?: Microservice, opts?: MicroserviceORMOptions) {
        this.connection = null;
        this._connectionName = opts?.connectionName;
        if (opts?.connectionOptions)
            this._connectionOptions = opts.connectionOptions;
        if (opts?.connectionRequired !== undefined)
            this.connectionRequired = opts.connectionRequired;
        if (opts?.unhealthyWithoutConnection !== undefined)
            this.unhealthyWithoutConnection = opts.unhealthyWithoutConnection;
        if (opts?.connectionAttemptInterval)
            this.connectionAttemptInterval = opts.connectionAttemptInterval;
        if (opts?.healthCheckQuery)
            this.healthCheckQuery = opts.healthCheckQuery;
        if (opts?.config)
            this.config = opts.config;
        if (app)
            this.bindTo(app);
    }

    private async getConnectionOptions(): Promise<ConnectionOptions> {
        let options: ConnectionOptions | undefined = this._connectionOptions;
        if (!options)
            options = await this.setConnectionOptions(await getConnectionOptions(this._connectionName));

        return options;
    }

    setConnectionOptions(options: ConnectionOptions): ConnectionOptions {
        if (options.name)
            this._connectionName = options.name;
        this._connectionOptions = options;
        return options;
    }

    bindTo(app: Microservice) {
        this.app = app;
        this.app.onStartup(this.connect.bind(this));
        this.app.onShutdown(this.close.bind(this));
        this.app.addHealthCheck(this.healthCheck.bind(this));
    }

    private async connect(): Promise<boolean> {
        if (this.connection?.isConnected)
            return true;
        try {
            this.app?.info('ORM:','Attempting Connection');
            if (!this.connection && this._connectionOptions)
                this.connection = await createConnection(this._connectionOptions)

            if (!this.connection && this._connectionName)
                this.connection = await createConnection(this._connectionName);

            if (!this.connection)
                this.connection = await createConnection();

            if (!this.connection.isConnected)
                await this.connection.connect();

            if (this.connection?.isConnected)
                this.app?.info('ORM:','Connecting');

            return this.connectionRequired ? this.connection?.isConnected : true;
        } catch (err) {
            this.app?.error('ORM:','Error',err.message);
            return this.connectionRequired ? false : true;
        }
    }
    private async close(): Promise<boolean> {
        if (!this.connection)
            return true;

        if (!this.connection.isConnected)
            return true;

        await this.connection.close();
        return this.connection.isConnected ? false : true;
    }

    private async healthCheck(): Promise<boolean> {

        // if we don't care for a database connection then return true;
        if (this.unhealthyWithoutConnection === false)
            return true;

        // use last health check if interval has not passed
        if (this._lastHealthCheck && this._lastHealthCheckResult)
            if (this._lastHealthCheck.getTime() >= (new Date().getTime()-this.healthCheckInterval))
                return this._lastHealthCheckResult

        // call connect incase databased needs connection
        await this.connect();

        // return false if no connection exists or connection is not connection
        if (!this.connection || !this.connection?.isConnected)
            return false;

        // perform a check query
        let result: boolean = false;
        try {
            const check = await this.connection.query(this.healthCheckQuery);
            result =  (check.length > 0);
        } catch (err) {
            result = false;
        } finally {
            this._lastHealthCheck = new Date();
            this._lastHealthCheckResult = result
        }
        return result;
    }

    buildConnectionOptions(config?: MicroserviceORMConfig): MicroserviceORMConnectionOptions {
        const output = {
            cli: {
                entitiesDir: "./src/entity",
                migrationsDir: "./src/migration",
                subscribersDir: "./src/subscriber"
            },
            entities: ["./build/entity/*.js","./build/entity/**/*.js"],
            subscribers: ["./build/subscriber/*.js","./build/subscriber/**/*.js"],
            migrations: ["./build/migration/*.js"],
            type: config?.type || this.config?.type || 'mysql'
        };
        const details = {
            port: config?.port || this.config?.port || 3306,
            username: config?.username || this.config?.username,
            password: config?.password || this.config?.password,
            database: config?.database || this.config?.database,
        }

        const master: string = config?.master || this.config?.master || '127.0.0.1';
        let slaves: string | string[] = config?.slaves || this.config?.slaves || []
        if (typeof slaves === "string")
            slaves = [slaves];

        // return single server if no replication
        if (slaves.length === 0) {
            return {
                host: master,
                ...output,
                ...details
            }
        }

        // handle replication
        const replicationSlaves = [];
        let slave: string;
        for (slave of slaves) {
            replicationSlaves.push({
                host: slave,
                ...details
            })
        }

        return {
            ...output,
            replication: {
                master: {
                    host: master,
                    ...details
                },
                slaves: replicationSlaves
            }
        }
    }

    async getConnection(): Promise<Connection> {
        if (!this.connection || !this.connection.isConnected)
            await this.connect();

        if (this.connection?.isConnected)
            return this.connection;

        return this._connectionName ? getConnection(this._connectionName) : getConnection();
    }
}
