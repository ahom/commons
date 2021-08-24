export class SecurityContext {
    constructor(
        public readonly props: {
            readonly applicationId?: string,
            readonly subscriptionId?: string,
            readonly userId?: string,
            readonly scopes?: string[]
        }
    ) {}

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
        return !!this.props.subscriptionId;
    }

    static fromAuthorizer(authorizer: any): SecurityContext {
        return new SecurityContext({
            applicationId: authorizer?.lambda?.applicationId,
            subscriptionId: authorizer?.lambda?.subscriptionId,
            userId: authorizer?.lambda?.userId,
            scopes: authorizer?.lambda?.scopes
        });
    }
}