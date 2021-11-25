FROM nodered/node-red

COPY settings.js /data/settings.js
COPY flows_cred.json /data/flows_cred.json
COPY flows.json /data/flows.json

WORKDIR /usr/src/node-red

RUN npm install node-red-contrib-web-worldmap
RUN npm install node-red-dashboard
RUN npm install node-red-node-mysql
RUN npm install node-red-node-ui-iframe


