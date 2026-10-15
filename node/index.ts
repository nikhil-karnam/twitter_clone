import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import _ from "lodash";

const proto: any = grpc.loadPackageDefinition(protoLoader.loadSync("api.proto", { arrays: true })).api;
const api = new proto.Api(process.env.API_ADDR, grpc.credentials.createInsecure());

async function signup(prefix: string, count: number) {
  const existing: string[] = await new Promise((done) => api.GetUsers({}, (err: any, res: any) => done(res.users)));
  const start = existing.filter((name) => name.startsWith(prefix)).length;
  const users = _.range(start, start + count).map((i) => prefix + i);
  const emails = users.map((name) => name + "@test.com");

  await new Promise((done) => api.Signup({ users, emails }, done));
}

export async function handler(event: any) {
  const count = Number(event.queryStringParameters.count);
  if (event.rawPath === "/signup-randos") await signup("user", count);
  if (event.rawPath === "/signup-celebs") await signup("celeb", count);
}
