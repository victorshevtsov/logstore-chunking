import { convertBytesToStreamMessage } from '@streamr/trackerless-network';
import { Logger } from '@streamr/utils';
import { PassThrough, pipeline, Readable } from 'stream';

import { Chunker, ChunkerCallback } from './Chunker';
import { DatabaseAdapter } from './DatabaseAdapter';
import {
	MAX_SEQUENCE_NUMBER_VALUE,
	MAX_TIMESTAMP_VALUE,
	MIN_SEQUENCE_NUMBER_VALUE,
} from './LogStore';
import { PropagationList } from './PropagationList';
import { QueryRequest, QueryType } from './protocol/QueryRequest';
import { QueryResponse } from './protocol/QueryResponse';

const logger = new Logger(module);

export class Propagator {
	private readonly database: DatabaseAdapter;
	private readonly queryRequest: QueryRequest;

	private readonly propagationList: PropagationList;
	private readonly propagationStream: PassThrough;
	private readonly queryStreams: Set<Readable> = new Set<Readable>();

	constructor(
		database: DatabaseAdapter,
		queryRequest: QueryRequest,
		responseChunkCallback: ChunkerCallback<Uint8Array>,
		propagationChunkCallback: ChunkerCallback<Uint8Array>,
		closeCallback: () => void
	) {
		this.database = database;
		this.queryRequest = queryRequest;

		this.propagationList = new PropagationList();
		this.propagationStream = new PassThrough({ objectMode: true });
		this.queryStreams = new Set<Readable>();

		let queryStream: Readable;
		switch (this.queryRequest.queryOptions.queryType) {
			case QueryType.Last:
				queryStream = this.database.queryLast(
					this.queryRequest.streamId,
					this.queryRequest.partition,
					this.queryRequest.queryOptions.last
				);
				break;
			case QueryType.From:
				queryStream = this.database.queryRange(
					this.queryRequest.streamId,
					this.queryRequest.partition,
					this.queryRequest.queryOptions.from.timestamp,
					this.queryRequest.queryOptions.from.sequenceNumber ??
						MIN_SEQUENCE_NUMBER_VALUE,
					MAX_TIMESTAMP_VALUE,
					MAX_SEQUENCE_NUMBER_VALUE,
					this.queryRequest.queryOptions.publisherId,
					undefined
				);
				break;
			case QueryType.Range:
				queryStream = this.database.queryRange(
					this.queryRequest.streamId,
					this.queryRequest.partition,
					this.queryRequest.queryOptions.from.timestamp,
					this.queryRequest.queryOptions.from.sequenceNumber ??
						MIN_SEQUENCE_NUMBER_VALUE,
					this.queryRequest.queryOptions.to.timestamp,
					this.queryRequest.queryOptions.to.sequenceNumber ??
						MAX_SEQUENCE_NUMBER_VALUE,
					this.queryRequest.queryOptions.publisherId,
					this.queryRequest.queryOptions.msgChainId
				);
				break;
		}

		queryStream.on('data', (bytes: Uint8Array) => {
			const message = convertBytesToStreamMessage(bytes);
			this.propagationList.pushForeign(message.messageId.toMessageRef());
			this.doCheck();
		});
		queryStream.on('end', () => {
			this.propagationList.fnalizeForeign();
			this.doCheck();
		});

		pipeline(
			queryStream,
			new Chunker<Uint8Array>(responseChunkCallback),
			(err) => {
				if (err) {
					// TODO: Handle error
					logger.error('Pipeline failed', { err });
				}
			}
		);

		pipeline(
			this.propagationStream,
			new Chunker<Uint8Array>(propagationChunkCallback),
			(err) => {
				if (err) {
					// TODO: Handle error
					logger.error('Pipeline failed', { err });
				}

				closeCallback();
			}
		);
	}

	public onPrimaryResponse(response: QueryResponse) {
		response.messageRefs.forEach((messageRef) => {
			this.propagationList.pushPrimary(messageRef);
		});

		if (response.isFinal) {
			this.propagationList.fnalizePrimary();
		}

		this.doCheck();
	}

	private doCheck() {
		const messaqgeRefs = this.propagationList.getDiffAndSrink();

		if (messaqgeRefs.length) {
			const queryStream = this.database.queryByMessageRefs(
				this.queryRequest.streamId,
				this.queryRequest.partition,
				messaqgeRefs
			);
			this.queryStreams.add(queryStream);

			queryStream.on('end', () => {
				this.queryStreams.delete(queryStream);
				this.doCheck();
			});
			queryStream.on('error', (err) => {
				// TODO: Handle error
				logger.error('query failed', { err });
			});

			queryStream.pipe(this.propagationStream, {
				end: this.propagationList.isFinalized,
			});
		} else if (
			this.propagationList.isFinalized &&
			this.queryStreams.size === 0
		) {
			this.propagationStream.end();
		}

		// TODO: Handle an errror if the pipe gets broken
	}
}
