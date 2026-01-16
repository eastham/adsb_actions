"""Represents the layout of the flight monitor screen."""

from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.button import Button
from kivy.uix.label import Label
from kivy.app import App
from kivy.metrics import dp

WHITE = (1,1,1,1)
BLACK = (0,0,0,1)
RED = (1,0,0,1)
GREEN = (0,.6,0,1)

class FlightMonitor(GridLayout):
    def __init__(self, **kwargs):
        super(FlightMonitor, self).__init__(**kwargs)
        self.NUM_SECTIONS = 3   # Scenic, Arrivals, Departures
        self.ROWS_PER_SECTION = 5  # number of flights per section

        self.cols = 1  # Single column layout
        self.button_count = 30 # Number of buttons/rows
        self.rowstore = []  # Store the rows so we can change them later

        self.add_header_row('Welcome to 88NV -- serving up ample delays since 2000')

        for i in range(self.NUM_SECTIONS):
            self.add_header_row("", RED)

            if i == 0:
                self.add_header_row("Scenic Flights", RED)
            elif i == 1:
                self.add_header_row("Arrivals", RED)
            elif i == 2:
                self.add_header_row("Departures", RED)

            self.add_content_row(BLACK, text_tuple=('Flight', 'Tail #', 'Location'))

            for i in range(self.ROWS_PER_SECTION):
                colorval = .3
                # alternate colors each row
                colorval += (i % 2) * .3
                bgcolortuple = (colorval, colorval, colorval, 1)

                grid = self.add_content_row(bgcolortuple, color=GREEN)
                self.rowstore.append(grid)

    def generate_button(self, bgcolor, color=WHITE, text=None):
        """Create a button with an optional text."""

        if text is None:
            text = ''
        return Button(font_name='Silkscreen-Regular.ttf', text=text,
                      background_color=bgcolor, color=color)

    def add_content_row(self, bgcolor, color=WHITE, text_tuple=None):
        """Create a 3-column-wide layout, for the three pieces of 
        flight data"""

        grid = GridLayout(cols=3)
        for i in range(3):
            if text_tuple:
                button = self.generate_button(
                    bgcolor, color=color, text=text_tuple[i])
                # XXX doesn't work to left-justify text, text size not yet established:
                # button.text_size = button.size
                grid.add_widget(button)
            else:
                grid.add_widget(self.generate_button(bgcolor, color=color))

        self.add_widget(grid)
        return grid

    def add_header_row(self, text, color=WHITE):
        """window-spanning headline row"""

        button = self.generate_button(text=text, bgcolor=BLACK, color=color)
        self.add_widget(button)

    def change_button_text(self, index, text_tuple):
        """Set the text of the row at the given index."""

        print(f"change button {index} to {text_tuple}")
        row = self.rowstore[index]
        buttons = row.children
        text_tuple = reversed(text_tuple)
        for i, text in enumerate(text_tuple):
            buttons[i].text = text
            buttons[i].text_size = buttons[i].size  # left-justify text
