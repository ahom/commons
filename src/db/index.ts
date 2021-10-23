import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { ListCommandOptions, ListCommand } from './commands';

export interface IndexProps<H, S, HT, ST> {
  hashTransform: (input: H) => HT,
  sortTransform: (input: S) => ST
}

export class Index<H, S, HT, ST> {
  constructor(
    private readonly dynamoDBClient: DynamoDBClient,
    private tableName: string,
    private indexName: string,
    private props: IndexProps<H, S, HT, ST>
  ) {
  }

  listCommand(hashKey: H, options?: Omit<ListCommandOptions<ST, any>, 'indexName'>) {
    return new ListCommand(
      this.dynamoDBClient,
      this.tableName,
      this.props.hashTransform(hashKey),
      {
        ...(options ?? {}),
        indexName: this.indexName
      }
    );
  }
}

export class IndexBuilder<H, S, HT, ST> {
  constructor(
    private readonly dynamoDBClient: DynamoDBClient,
    private tableName: string,
    private indexName: string,
    private props: IndexProps<H, S, HT, ST>
  ) {}

    withKeyTransform<NH, NS>(
        hashTransform: (input: NH) => HT,
        sortTransform: (input: NS) => ST
    ) {
        return new IndexBuilder(
            this.dynamoDBClient,
            this.tableName,
            this.indexName,
            {
                hashTransform: hashTransform,
                sortTransform: sortTransform
            }
        );
    }

  build() {
    return new Index(
      this.dynamoDBClient,
      this.tableName,
      this.indexName,
      this.props
    );
  }
}

export function index<H, S>(
  dynamoDBClient: DynamoDBClient,
  tableName: string,
  indexName: string 
) {
  return new IndexBuilder<H, S, H, S>(
    dynamoDBClient,
    tableName,
    indexName,
    {
      hashTransform: x => x,
      sortTransform: x => x
    }
  );
}
