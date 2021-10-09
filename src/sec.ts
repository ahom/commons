export interface AuthorizerContext {
    applicationId?: string,
    subscription?: {
        id: string,
        ownerId: string,
        itemId: string
    }
    userId?: string,
    email?: string,
    scopes?: string[]
}

export class SecurityContext {
    constructor(public readonly props: AuthorizerContext) {}

    isScopeAuthorized(scopeName: string): boolean {
        if (!this.props.scopes) return false;
        return this.props.scopes.some(authorizedScope => isScopeIncludedIn(scopeName, authorizedScope));
    }

    hasActiveSubscription(): boolean {
        return !!this.props.subscription;
    }

    hasValidUser(): boolean {
        return !!this.props.userId;
    }

    static fromAuthorizer(authorizer?: { lambda?: AuthorizerContext }): SecurityContext {
        return new SecurityContext({
            applicationId: authorizer?.lambda?.applicationId,
            subscription: authorizer?.lambda?.subscription,
            userId: authorizer?.lambda?.userId,
            email: authorizer?.lambda?.email,
            scopes: authorizer?.lambda?.scopes
        });
    }
}

export function isScopeIncludedIn(scope: string, referenceScope: string): boolean {
    return (
        referenceScope.endsWith('*') && scope.startsWith(referenceScope.substring(0, referenceScope.length - 1))
    ) || (scope === referenceScope);
}

export function filterScope(scope: string, filteringScope: string): string | undefined {
    return isScopeIncludedIn(scope, filteringScope) ? scope : (
        isScopeIncludedIn(filteringScope, scope) ? filteringScope : undefined
    );
}

export function filterScopes(scopes: string[], filteringScopes: string[]): string[] {
    return scopes.flatMap(scope => filteringScopes.map(
            filteringScope => filterScope(scope, filteringScope)
        ).filter(scope => !!scope)
    );
}