# Use Node.js LTS version
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of the application
COPY . .

# Expose the port the app runs on
EXPOSE 5001

# Set environment to development
ENV NODE_ENV=development
ENV PORT=5001

# Start the development server
CMD ["npm", "run", "dev"]

