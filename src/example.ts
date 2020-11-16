import {Microservice} from "koa-microservice";
import {MicroserviceORM} from "./index";


const app = new Microservice({
    useConsole: true
});

const orm = new MicroserviceORM(app);
orm.connectionRequired = false;

app.healthCheckEndpoint = '/healthy';
app.http(8080);
app.start();

