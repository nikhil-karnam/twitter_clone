import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import _ from "lodash";

const proto: any = grpc.loadPackageDefinition(protoLoader.loadSync("api.proto")).api;
const api = new proto.Api(process.env.API_ADDR, grpc.credentials.createInsecure());

async function load() {
  const randos = _.range(10000).map((i) => "user" + i);
  const celebs = _.range(10).map((i) => "celeb" + i);
  const everyone = [...randos, ...celebs];
  const emails = everyone.map((name) => name + "@test.com");

  await new Promise((done) => api.Signup({ users: everyone, emails }, done));

  for (const user of randos) {
    const targets = [..._.sampleSize(_.without(randos, user), 100), ...celebs];
    const users = targets.map(() => user);
    await new Promise((done) => api.Follow({ users, targets }, done));
  }
}

export async function handler(event: any) {
  if (event.rawPath === "/load") await load();
}