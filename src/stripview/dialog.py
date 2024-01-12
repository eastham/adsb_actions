from kivymd.uix.floatlayout import MDFloatLayout
from kivymd.uix.dialog import MDDialog
from kivymd.uix.button import MDFlatButton, MDRaisedButton
from kivy.lang.builder import Builder
from kivy.uix.boxlayout import BoxLayout

kv_multi = '''
<Content>:
    orientation: "vertical"
    spacing: "12dp"
    size_hint: (1, None)
    height:200

    MDBoxLayout:
        id: arrivals
        orientation: "vertical"
        adaptive_height: True

        MDTextField:
            focus: True
            id : pin
            on_text_validate: root.enter_pressed()
            height: "30dp"
        MDLabel:
            text: "test"
        MDTextField:
            focus: True
            id : pin
            on_text_validate: root.enter_pressed()
            height: "30dp"

'''
kv = '''
<Content>:
    orientation: "vertical"
    spacing: "12dp"
    size_hint: (1, None)

    MDTextField:
        focus: True
        id : pin
        on_text_validate: root.enter_pressed()
        height: "30dp"

'''
class Content(BoxLayout):
    def __init__(self, on_enter_cb):
        self.on_enter_cb = on_enter_cb
        super().__init__()

    def enter_pressed(self):
        self.on_enter_cb(None)

class Dialog:
    cdialog = None

    def __init__(self):
        self.app = None
        self.id = None
        Builder.load_string(kv)

    def show_custom_dialog(self, app, id):
        self.app = app
        self.id = id
        on_enter_cb = lambda x:self.get_data(x,content_cls)
        content_cls = Content(on_enter_cb)
        self.cdialog = MDDialog(title='Enter Code', content_cls=content_cls,
            type="custom", buttons = [
                MDFlatButton(text="Cancel",on_release=lambda x: self.close_dialog()),
                MDRaisedButton(text="Ok",on_release=on_enter_cb)
            ])
        self.cdialog.open()

    def close_dialog(self):
        if self.cdialog:
            self.cdialog.dismiss()

    def get_data(self, instance_btn, content_cls):
        textfield = content_cls.ids.pin
        value = textfield._get_text()
        self.app.annotate_strip(self.id, value)
        self.app.set_strip_color(self.id, (.4,.4,.4))
        self.close_dialog()
