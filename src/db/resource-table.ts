import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { IndexProps } from './index';
import { uuid } from '../utils';
import { UpdateCommandOptions, DeleteCommandOptions, ListCommandOptions, CreateCommand, RetrieveCommand, UpdateCommand, DeleteCommand, ListCommand, RetrieveCommandOptions, BatchWriteCommand, FullScanCommandOptions, FullScanCommand } from './commands';

export interface Resource<AttributesType extends Object> {
    readonly id: string,
    readonly attributes: AttributesType,
    readonly meta: { 
        readonly etag: string,
        readonly created_at: string,
        readonly created_by: string,
        readonly last_updated_at: string,
        readonly last_updated_by: string
    }
}

export function filterResourceFields<AttributesType extends Object>(rsc: Resource<AttributesType>) {
    const { id, attributes, meta, ...rest } = rsc;
    return {
        id, attributes, meta
    };
}

export interface ResourceTableProps<H, S, HT, ST, A> extends IndexProps<H, S, HT, ST> {
    transform?: (key: H & S, attr: A) => any,
    doNotIncludeMeta?: boolean,
    doNotIncludeKeys?: boolean
}

export class ResourceTable<H, S, HT, ST, A> {
    constructor(
        private readonly dynamoDBClient: DynamoDBClient,
        public readonly tableName: string,
        private readonly props: ResourceTableProps<H, S, HT, ST, A>
    ) {}

    buildKey(key: H & S): HT & ST {
        return {
            ...this.props.sortTransform(key),
            ...this.props.hashTransform(key)
        }
    }

    buildItem(id: string, key: H & S, attributes: A, userId: string, ttl?: Date) {
        const now = new Date().toISOString();
        return {
            ...(this.props.doNotIncludeKeys ? {} : key),
            ...(this.props.transform ? this.props.transform(key, attributes) : {}),
            id: id, 
            attributes: attributes,
            ...(this.props.doNotIncludeMeta ? {} : {
                meta: {
                    etag: `"${uuid()}"`,
                    created_at: now,
                    created_by: userId,
                    last_updated_at: now,
                    last_updated_by: userId
                }
            }),
            ...(ttl ? {
                ttl: Math.ceil(ttl.getTime() / 1000)
            } : {})
        };
    }

    createCommand(id: string, key: H & S, attributes: A, userId: string, ttl?: Date ) {
        return new CreateCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            this.buildKey(key),
            this.buildItem(id, key, attributes, userId, ttl)
        );
    }

    batchWriteCommand(items: {id: string, key: H & S, attributes: A, userId: string, ttl?: Date}[]) {
        return new BatchWriteCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            items.map(
                item => ({
                    key: this.buildKey(item.key),
                    value: this.buildItem(item.id, item.key, item.attributes, item.userId, item.ttl)
                })
            )
        );
    }

    retrieveCommand(key: H & S, options?: RetrieveCommandOptions) {
        return new RetrieveCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            this.buildKey(key),
            options
        );
    }

    updateCommand(key: H & S, attributes: A, userId: string, options?: UpdateCommandOptions, ttl?: Date) {
        return new UpdateCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            this.buildKey(key),
            {
                ...(this.props.transform ? this.props.transform(key, attributes) : {}),
                attributes: attributes,
                ...(this.props.doNotIncludeMeta ? {} : {
                    'meta.last_updated_by': userId,
                    'meta.last_updated_at': new Date().toISOString(),
                    'meta.etag': `"${uuid()}`
                }),
                ...(ttl ? {
                    ttl: Math.ceil(ttl.getTime() / 1000)
                } : {})
            }, 
            options
      );
    }

    deleteCommand(key: H & S, options?: DeleteCommandOptions) {
        return new DeleteCommand(
            this.dynamoDBClient,
            this.tableName,
            this.buildKey(key),
            options
        );
    }

    listCommand(hashKey: H, options?: Omit<ListCommandOptions<ST, A>, 'indexName'>) {
        return new ListCommand<HT, ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            this.props.hashTransform(hashKey),
            options,
            'attributes'
        );
    }

    fullScanCommand(options?: FullScanCommandOptions) {
        return new FullScanCommand<HT, ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            options
        );
    }
}

export class ResourceTableBuilder<H, S, HT, ST, A> {
    constructor(
        private readonly dynamoDBClient: DynamoDBClient,
        private readonly tableName: string,
        private readonly props: ResourceTableProps<H, S, HT, ST, A>
    ) {}

    withKeyTransform<NH, NS>(
        hashTransform: (input: NH) => HT,
        sortTransform: (input: NS) => ST
    ) {
        if (this.props.transform) {
            throw new Error('withHashTransform should be called before withTransform');
        }
        return new ResourceTableBuilder<NH, NS, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props,
                transform: undefined,
                hashTransform: hashTransform,
                sortTransform: sortTransform
            }
        );
    }

    withoutIncludedMeta() {
        return new ResourceTableBuilder<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props,
                doNotIncludeMeta: true
            }
        );
    }

    withoutIncludedKeys() {
        return new ResourceTableBuilder<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props,
                doNotIncludeKeys: true
            }
        );
    }


    withTransform(
        transform: (key: H & S, attr: A) => any
    ) {
        return new ResourceTableBuilder<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props,
                transform
            }
        );
    }

    build() {
        return new ResourceTable<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            this.props
        );
    }
}

export function resourceTable<H, S, A = Object>(
    dynamoDBClient: DynamoDBClient,
    tableName: string
) {
    return new ResourceTableBuilder<H, S, H, S, A>(
        dynamoDBClient,
        tableName,
        {
            hashTransform: x => x,
            sortTransform: x => x
        }
    );
}