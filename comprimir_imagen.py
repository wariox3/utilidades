from PIL import Image
import os

def compress_with_pillow(input_path, output_path, quality=70, optimize=True):
    """
    Comprime una imagen usando Pillow.
    
    :param input_path: Ruta de la imagen original.
    :param output_path: Ruta donde se guardará la imagen comprimida.
    :param quality: Calidad (1-100). Menor = más compresión.
    :param optimize: Habilita optimización adicional (mejor compresión).
    """
    try:
        with Image.open(input_path) as img:
            # Convertir a RGB si es PNG con transparencia (JPEG no soporta alpha)
            if img.mode in ('RGBA', 'P'):
                img = img.convert('RGB')
            
            # Guardar con compresión
            img.save(
                output_path,
                quality=quality,
                optimize=optimize,
                # Para PNG, usa `compress_level` (1-9, 9=máxima compresión)
                **({'compress_level': 9} if output_path.lower().endswith('.png') else {})
            )
        
        original_size = os.path.getsize(input_path) / 1024  # KB
        compressed_size = os.path.getsize(output_path) / 1024  # KB
        print(f"✅ Compresión exitosa: {original_size:.2f} KB → {compressed_size:.2f} KB")
    
    except Exception as e:
        print(f"❌ Error: {e}")

# Ejemplo de uso
compress_with_pillow("/home/desarrollo/Escritorio/6030820.jpg", "/home/desarrollo/Escritorio/6030820_c.jpg", quality=10)
#compress_with_pillow("imagen_original.png", "imagen_comprimida.png")