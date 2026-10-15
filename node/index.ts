import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import _ from "lodash";

const proto: any = grpc.loadPackageDefinition(protoLoader.loadSync("api.proto", { arrays: true })).api;
const api = new proto.Api(process.env.API_ADDR, grpc.credentials.createInsecure());

function getUsers(): Promise<string[]> {
  return new Promise((done) => api.GetUsers({}, (err: any, res: any) => done(res.users)));
}

async function signup(count: number) {
  const everyone = await getUsers();
  const users = _.range(everyone.length, everyone.length + count).map((i) => "user" + i);
  const emails = users.map((name) => name + "@test.com");

  await new Promise((done) => api.Signup({ users, emails }, done));
}

async function follow(count: number) {
  const everyone = await getUsers();
  const celebs = everyone.slice(-10);
  const randos = everyone.slice(0, -10);

  for (const user of randos) {
    const targets = [..._.sampleSize(_.without(randos, user), count), ...celebs];
    const users = targets.map(() => user);
    await new Promise((done) => api.Follow({ users, targets }, done));
  }
}

async function setThreshold(value: number) {
  await new Promise((done) => api.SetThreshold({ value }, done));
}

export async function handler(event: any) {
  const count = Number(event.queryStringParameters.count);
  const value = Number(event.queryStringParameters.value);
  if (event.rawPath === "/signup") await signup(count);
  if (event.rawPath === "/follow") await follow(count);
  if (event.rawPath === "/threshold") await setThreshold(value);
}
