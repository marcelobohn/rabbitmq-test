FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
# Each service overrides this command via docker-compose.yml
CMD ["node", "--version"]
