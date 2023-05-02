FROM node:19 AS ui-build
WORKDIR /usr/src/app
COPY . .
EXPOSE 3000
RUN yarn webpack build

CMD ["yarn", "webpack-start", "--host", "0.0.0.0", "--port", "3000"]
