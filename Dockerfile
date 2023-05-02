FROM node:19 AS ui-build
WORKDIR /usr/src/app
COPY . .
EXPOSE 3000

CMD ["yarn", "start"]
