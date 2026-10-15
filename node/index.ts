import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import _ from "lodash";

const proto: any = grpc.loadPackageDefinition(protoLoader.loadSync("api.proto", { arrays: true })).api;
const api = new proto.Api(process.env.API_ADDR, grpc.credentials.createInsecure());


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

async function tweet(userType: string, burstMode: boolean) {
  const users = userType === "celebs" ? (await getCelebs()).flatMap((celeb) => _.times(10, () => celeb)) : await getRandos();
  const tweets = users.map(() => "howdy world");
  const method = burstMode ? "BurstTweet" : "SequentialTweet";

  await new Promise((done) => api[method]({ users, tweets }, done));
}

//getters
function getUsers(): Promise<string[]> {
  return new Promise((done) => api.GetUsers({}, (err: any, res: any) => done(res.users)));
}

function getCelebs(): Promise<string[]> {
  return new Promise((done) => api.GetCelebs({}, (err: any, res: any) => done(res.users)));
}

function getRandos(): Promise<string[]> {
  return new Promise((done) => api.GetRandos({}, (err: any, res: any) => done(res.users)));
}

function openMail(user: string): Promise<string[]> {
  return new Promise((done) => api.OpenMail({ user }, (err: any, res: any) => done(res.tweets)));
}

function getProfile(user: string): Promise<string[]> {
  return new Promise((done) => api.GetProfile({ user }, (err: any, res: any) => done(res.tweets)));
}

export async function handler(event: any) {
  if (event.rawPath === "/signup") await signup(Number(event.queryStringParameters.amount));
  if (event.rawPath === "/follow") await follow(Number(event.queryStringParameters.amount));
  if (event.rawPath === "/threshold") await setThreshold(Number(event.queryStringParameters.threshold));
  if (event.rawPath === "/tweet") await tweet(event.queryStringParameters.userType, event.queryStringParameters.burstMode === "true");
  if (event.rawPath === "/feed") return await openMail(event.queryStringParameters.user);
  if (event.rawPath === "/profile") return await getProfile(event.queryStringParameters.user);
}