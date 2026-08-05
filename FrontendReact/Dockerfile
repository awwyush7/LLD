# Stage 1 — Build
# Use Node to install dependencies and build the React app into static files
FROM node:20-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
# /app/dist now contains index.html + compiled JS/CSS, nothing else needed

# Stage 2 — Serve
# Throw away Node entirely. Copy only the /dist output into a tiny nginx image.
FROM nginx:alpine

COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
