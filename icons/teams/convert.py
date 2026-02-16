from PIL import Image
import os

def resize_to_square_png(input_path, output_path, size=200):
    img = Image.open(input_path).convert("RGBA")

    # Keep aspect ratio
    img.thumbnail((size, size), Image.LANCZOS)

    # Create transparent background
    new_img = Image.new("RGBA", (size, size), (0, 0, 0, 0))

    # Center image
    x = (size - img.width) // 2
    y = (size - img.height) // 2
    new_img.paste(img, (x, y), img)

    new_img.save(output_path, "PNG")

# Example
for file in os.listdir("."):
    if file.endswith(".png"):
        resize_to_square_png(file, file)
