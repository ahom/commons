export interface AuthorizerContext {
    readonly applicationId?: string,
    readonly subscriptionId?: string,
    readonly subscriptionOwnerId?: string,
    readonly userId?: string,
    readonly scopes?: string[]
}

export class SecurityContext {
    constructor(public readonly props: AuthorizerContext) {}

    isScopeAuthorized(scopeName: string): boolean {
        if (!this.props.scopes) return false;

        for (let authorizedScope of this.props.scopes) {
            if (authorizedScope.endsWith('*')) {
                if (scopeName.startsWith(authorizedScope.substring(0, authorizedScope.length - 1))) {
                    return true;
                }
            } else {
                if (scopeName === authorizedScope) {
                    return true;
                }
            }
        }

        return false;
    }

    hasActiveSubscription(): boolean {
        return !!this.props.subscriptionId && !!this.props.subscriptionOwnerId;
    }

    hasValidUser(): boolean {
        return !!this.props.userId;
    }

    static fromAuthorizer(authorizer?: { lambda?: AuthorizerContext }): SecurityContext {
        return new SecurityContext({
            applicationId: authorizer?.lambda?.applicationId,
            subscriptionId: authorizer?.lambda?.subscriptionId,
            subscriptionOwnerId: authorizer?.lambda?.subscriptionOwnerId,
            userId: authorizer?.lambda?.userId,
            scopes: authorizer?.lambda?.scopes
        });
    }
}