const { app } = require('./app');
//const port = 3000;
const port = process.env.PORT || 3000;
app.listen(port, () => {});