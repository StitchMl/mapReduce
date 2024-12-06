package utils

// ANSI escape codes for colors
const (
	// RESET Reset
	RESET = "\033[0m" // Text Reset

	// BLACK and the other are Regular Colors
	BLACK   = "\033[0;30m" // BLACK
	RED     = "\033[0;31m" // RED
	GREEN   = "\033[0;32m" // GREEN
	YELLOW  = "\033[0;33m" // YELLOW
	BLUE    = "\033[0;34m" // BLUE
	PURPLE  = "\033[0;35m" // PURPLE
	CYAN    = "\033[0;36m" // CYAN
	WHITE   = "\033[0;37m" // WHITE
	MAGENTA = "\033[35m"   // MAGENTA

	// BlackBold and the other are Bold
	BlackBold  = "\033[1;30m" // BLACK
	RedBold    = "\033[1;31m" // RED
	GreenBold  = "\033[1;32m" // GREEN
	YellowBold = "\033[1;33m" // YELLOW
	BlueBold   = "\033[1;34m" // BLUE
	PurpleBold = "\033[1;35m" // PURPLE
	CyanBold   = "\033[1;36m" // CYAN
	WhiteBold  = "\033[1;37m" // WHITE

	// BlackUnderlined and the other are Underline
	BlackUnderlined  = "\033[4;30m" // BLACK
	RedUnderlined    = "\033[4;31m" // RED
	GreenUnderlined  = "\033[4;32m" // GREEN
	YellowUnderlined = "\033[4;33m" // YELLOW
	BlueUnderlined   = "\033[4;34m" // BLUE
	PurpleUnderlined = "\033[4;35m" // PURPLE
	CyanUnderlined   = "\033[4;36m" // CYAN
	WhiteUnderlined  = "\033[4;37m" // WHITE

	// BlackBackground and the other are Background
	BlackBackground  = "\033[40m" // BLACK
	RedBackground    = "\033[41m" // RED
	GreenBackground  = "\033[42m" // GREEN
	YellowBackground = "\033[43m" // YELLOW
	BlueBackground   = "\033[44m" // BLUE
	PurpleBackground = "\033[45m" // PURPLE
	CyanBackground   = "\033[46m" // CYAN
	WhiteBackground  = "\033[47m" // WHITE

	// BlackBright and the other are High Intensity
	BlackBright  = "\033[0;90m" // BLACK
	RedBright    = "\033[0;91m" // RED
	GreenBright  = "\033[0;92m" // GREEN
	YellowBright = "\033[0;93m" // YELLOW
	BlueBright   = "\033[0;94m" // BLUE
	PurpleBright = "\033[0;95m" // PURPLE
	CyanBright   = "\033[0;96m" // CYAN
	WhiteBright  = "\033[0;97m" // WHITE

	// BlackBoldBright and the other are Bold High Intensity
	BlackBoldBright  = "\033[1;90m" // BLACK
	RedBoldBright    = "\033[1;91m" // RED
	GreenBoldBright  = "\033[1;92m" // GREEN
	YellowBoldBright = "\033[1;93m" // YELLOW
	BlueBoldBright   = "\033[1;94m" // BLUE
	PurpleBoldBright = "\033[1;95m" // PURPLE
	CyanBoldBright   = "\033[1;96m" // CYAN
	WhiteBoldBright  = "\033[1;97m" // WHITE

	// BlackBackgroundBright and the other are High Intensity backgrounds
	BlackBackgroundBright  = "\033[0;100m" // BLACK
	RedBackgroundBright    = "\033[0;101m" // RED
	GreenBackgroundBright  = "\033[0;102m" // GREEN
	YellowBackgroundBright = "\033[0;103m" // YELLOW
	BlueBackgroundBright   = "\033[0;104m" // BLUE
	PurpleBackgroundBright = "\033[0;105m" // PURPLE
	CyanBackgroundBright   = "\033[0;106m" // CYAN
	WhiteBackgroundBright  = "\033[0;107m" // WHITE
)

// ColoredText applies a color to a given text
func ColoredText(color, text string) string {
	return color + text + RESET
}
