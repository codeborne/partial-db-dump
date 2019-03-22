fun main(args: Array<String>) {
  if (args.isEmpty()) {
    System.err.println("Usage: dbUrl")
    System.exit(1)
  }

  Migrator(args[0]).migrate()
}