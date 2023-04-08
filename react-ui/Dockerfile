FROM node:19 AS ui-build
WORKDIR /usr/src/app
COPY . .
RUN npm install && npm run build

EXPOSE 3000

CMD ["npm", "start"]
