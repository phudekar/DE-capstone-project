import { ApolloClient, InMemoryCache, HttpLink, split } from "@apollo/client";
import { GraphQLWsLink } from "@apollo/client/link/subscriptions";
import { getMainDefinition } from "@apollo/client/utilities";
import { createClient } from "graphql-ws";

const httpUrl = import.meta.env.VITE_GRAPHQL_HTTP_URL || "http://localhost:8000/graphql";
const wsUrl = import.meta.env.VITE_GRAPHQL_WS_URL || "ws://localhost:8000/graphql";

const httpLink = new HttpLink({ uri: httpUrl });

const wsLink = new GraphQLWsLink(
  createClient({
    url: wsUrl,
    retryAttempts: Infinity,
    shouldRetry: () => true,
    connectionParams: {},
  }),
);

const splitLink = split(
  ({ query }) => {
    const definition = getMainDefinition(query);
    return definition.kind === "OperationDefinition" && definition.operation === "subscription";
  },
  wsLink,
  httpLink,
);

export const apolloClient = new ApolloClient({
  link: splitLink,
  cache: new InMemoryCache(),
});
