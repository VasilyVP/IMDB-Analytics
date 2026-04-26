export class ApiError extends Error {
	status: number;

	constructor(status: number, message?: string) {
		super(message ?? `Request failed (${status})`);
		this.name = "ApiError";
		this.status = status;
	}
}
