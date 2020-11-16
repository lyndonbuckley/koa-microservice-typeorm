import {Microservice} from "koa-microservice";
import {Connection, createConnection, getConnection, getConnectionOptions} from "typeorm";
import {ConnectionOptions} from "typeorm/connection/ConnectionOptions";

export interface MicroserviceORMOptions {
    connectionName?: string;
    connectionOptions?: ConnectionOptions;
    connectionRequired?: boolean
    unhealthyWithoutConnection?: boolean
    connectionAttemptInterval?: number;
    healthCheckQuery?: string
    healthCheckInterval?: number;
}
export class MicroserviceORM {
    app?: Microservice;
    connection: Connection | null;

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
        if (!this.unhealthyWithoutConnection)
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

    async getConnection(): Promise<Connection> {
        if (!this.connection || !this.connection.isConnected)
            await this.connect();

        if (this.connection?.isConnected)
            return this.connection;

        return this._connectionName ? getConnection(this._connectionName) : getConnection();
    }
}
