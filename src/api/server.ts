import express from "express";
import orderRoutes from "./routes/order.routes";

const app = express();
const PORT = 3000;

// Middleware to parse JSON bodies from incoming requests to JavaScript objects
app.use(express.json());

/*
  Why _req instead of req?
  - The underscore prefix is a common convention in JavaScript and TypeScript to indicate that 
    a variable is intentionally unused.
  - In this case, _req indicates that the request object is not being used in the handler function, 
    which can help prevent linter warnings about unused variables.
*/

/*
  - You usually do not need to explicitly type req and res when the handler is written inline
    and TypeScript can infer them.
  - Once the function is defined outside app.get(...), TypeScript may not always know it is an Express 
    handler.
  - When you want to type req.params or req.body, you need to explicitly type the parameters 
    to ensure TypeScript understands the structure of the request.
*/

app.get("/health", (_req, res) => {
  res.status(200).json({
    success: true,
    message: "Order API is running.",
  });
});

app.use(orderRoutes);

app.listen(PORT, () => {
  console.log(`Order API running at http://localhost:${PORT}`);
});

/*
  - --transpile-only ( In pakage.json scripts)
  - It means: “Convert TypeScript to JavaScript and run it, but skip full type checking.”
*/