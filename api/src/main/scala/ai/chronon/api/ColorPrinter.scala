package ai.chronon.api

object ColorPrinter {
  // ANSI escape codes for text colors
  private val ANSI_RESET = "\u001B[0m"

  // Colors chosen for visibility on both dark and light backgrounds
  // More muted colors that should still be visible on various backgrounds
  private val ANSI_RED = "\u001B[38;5;131m" // Muted red (soft burgundy)
  private val ANSI_BLUE = "\u001B[38;5;32m" // Medium blue
  private val ANSI_YELLOW = "\u001B[38;5;172m" // Muted Orange
  private val ANSI_GREEN = "\u001B[38;5;28m" // Forest green

  private val BOLD = "\u001B[1m"

  implicit class ColorString(val s: String) extends AnyVal {
    def red: String = s"$ANSI_RED$s$ANSI_RESET"
    def blue: String = s"$ANSI_BLUE$s$ANSI_RESET"
    def yellow: String = s"$ANSI_YELLOW$s$ANSI_RESET"
    def green: String = s"$ANSI_GREEN$s$ANSI_RESET"
    def low: String = s.toLowerCase
    def highlight: String = s"$BOLD$ANSI_RED$s$ANSI_RESET"
  }
}
