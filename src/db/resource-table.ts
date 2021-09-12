import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { IndexProps } from './index';
import { uuid } from '../utils';
import { UpdateCommandOptions, DeleteCommandOptions, ListCommandOptions, CreateCommand, RetrieveCommand, UpdateCommand, DeleteCommand, ListCommand, RetrieveCommandOptions } from './commands';

export interface Resource<AttributesType extends Object> {
    readonly id: string,
    readonly type: string,
    readonly attributes: AttributesType,
    readonly meta: { 
        readonly etag: string,
        readonly created_at: string,
        readonly last_updated_at: string
    }
}

export function filterResourceFields<AttributesType extends Object>(rsc: Resource<AttributesType>) {
    const { id, type, attributes, meta, ...rest } = rsc;
    return {
        id, type, attributes, meta
    };
}

export interface ResourceTableProps<H, S, HT, ST, A> extends IndexProps<H, S, HT, ST> {
    transform?: (key: H & S, attr: A) => any
}

export class ResourceTable<H, S, HT, ST, A> {
    constructor(
        private readonly dynamoDBClient: DynamoDBClient,
        public readonly tableName: string,
        public readonly resourceType: string,
        private readonly props: ResourceTableProps<H, S, HT, ST, A>
    ) {}

    createCommand(id: string, key: H & S, attributes: A) {
        const now = new Date().toISOString();
        return new CreateCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props.sortTransform(key),
                ...this.props.hashTransform(key)
            },
            {
                ...key,
                ...(this.props.transform ? this.props.transform(key, attributes) : {}),
                id: id, 
                type: this.resourceType,
                attributes: attributes,
                meta: {
                    etag: `"${uuid()}"`,
                    created_at: now,
                    last_updated_at: now
                }
            }
        );
    }

    retrieveCommand(key: H & S, options?: RetrieveCommandOptions) {
        return new RetrieveCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props.sortTransform(key),
                ...this.props.hashTransform(key)
            },
            options
        );
    }

    updateCommand(key: H & S, attributes: A, options?: UpdateCommandOptions) {
        return new UpdateCommand<HT & ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props.sortTransform(key),
                ...this.props.hashTransform(key)
            },
            {
                ...(this.props.transform ? this.props.transform(key, attributes) : {}),
                attributes,
                'meta.last_updated_at': new Date().toISOString(),
                'meta.etag': `"${uuid()}`
            }, 
            options
      );
    }

    deleteCommand(key: H & S, options?: DeleteCommandOptions) {
        return new DeleteCommand(
            this.dynamoDBClient,
            this.tableName,
            {
                ...this.props.sortTransform(key),
                ...this.props.hashTransform(key)
            },
            options
        );
    }

    listCommand(hashKey: H, options?: Omit<ListCommandOptions<ST>, 'indexName'>) {
        return new ListCommand<HT, ST, H & S & Resource<A>>(
            this.dynamoDBClient,
            this.tableName,
            this.props.hashTransform(hashKey),
            options
        );
    }
}

export class ResourceTableBuilder<H, S, HT, ST, A> {
    constructor(
        private readonly dynamoDBClient: DynamoDBClient,
        private readonly tableName: string,
        private readonly resourceType: string,
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
            this.resourceType,
            {
                hashTransform: hashTransform,
                sortTransform: sortTransform
            }
        );
    }

    withTransform(
        transform: (key: H & S, attr: A) => any
    ) {
        return new ResourceTableBuilder<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            this.resourceType,
            {
                hashTransform: this.props.hashTransform,
                sortTransform: this.props.sortTransform,
                transform
            }
        );
    }

    build() {
        return new ResourceTable<H, S, HT, ST, A>(
            this.dynamoDBClient,
            this.tableName,
            this.resourceType,
            this.props
        );
    }
}

export function resourceTable<H, S, A = Object>(
    dynamoDBClient: DynamoDBClient,
    tableName: string,
    resourceType: string
) {
    return new ResourceTableBuilder<H, S, H, S, A>(
        dynamoDBClient,
        tableName,
        resourceType,
        {
            hashTransform: x => x,
            sortTransform: x => x
        }
    );
}