# Build stage
FROM node:20-alpine AS build

WORKDIR /app

ARG VITE_GRAPHQL_HTTP_URL=http://localhost:8000/graphql
ARG VITE_GRAPHQL_WS_URL=ws://localhost:8000/graphql

COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi

COPY . .
RUN npm run build

# Production stage
FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
