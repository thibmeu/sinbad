import { createHash, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type Server } from "node:http";

const MAX_BODY = 1024 * 1024;

export class HttpError extends Error {
	readonly status: number;
	constructor(status: number, message: string) {
		super(message);
		this.status = status;
	}
}

async function readBody(request: IncomingMessage): Promise<Buffer> {
	const chunks: Buffer[] = [];
	let size = 0;
	for await (const chunk of request as AsyncIterable<Buffer>) {
		size += chunk.length;
		if (size > MAX_BODY) throw new HttpError(413, "Request too large");
		chunks.push(chunk);
	}
	return Buffer.concat(chunks, size);
}

/** Serve a Web-style handler with node:http, so the handler stays portable. */
export function serve(
	handler: (request: Request) => Promise<Response>,
	port: number,
	host: string,
	name: string,
): Server {
	const server = createServer(async (incoming, outgoing) => {
		try {
			const headers = new Headers();
			for (const [key, value] of Object.entries(incoming.headers))
				if (typeof value === "string") headers.set(key, value);
			const method = incoming.method ?? "GET";
			const request = new Request(
				new URL(incoming.url ?? "/", `http://${host}:${port}`),
				{
					method,
					headers,
					...(method === "GET" || method === "HEAD"
						? {}
						: { body: new Uint8Array(await readBody(incoming)) }),
				},
			);
			const response = await handler(request);
			outgoing.writeHead(response.status, Object.fromEntries(response.headers));
			outgoing.end(Buffer.from(await response.arrayBuffer()));
		} catch (error) {
			if (!(error instanceof HttpError)) console.error(error);
			if (!outgoing.headersSent)
				outgoing.writeHead(error instanceof HttpError ? error.status : 500);
			outgoing.end();
		}
	});
	server.listen(port, host, () => console.log(`${name} listening on ${port}`));
	return server;
}

/** Compare bearer tokens over equal-length digests, in constant time. */
export function bearer(token: string): (request: Request) => void {
	const expected = createHash("sha256").update(`Bearer ${token}`).digest();
	return (request) => {
		const offered = createHash("sha256")
			.update(request.headers.get("authorization") ?? "")
			.digest();
		if (!timingSafeEqual(offered, expected))
			throw new HttpError(401, "Unauthorized");
	};
}

export function port(value: string | undefined, fallback: number): number {
	const result = Number(value ?? fallback);
	if (!Number.isInteger(result) || result < 1 || result > 65535)
		throw new Error("Invalid PORT");
	return result;
}
