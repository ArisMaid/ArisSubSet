DELETE FROM font_faces
WHERE EXISTS (
  SELECT 1
  FROM font_faces AS earlier
  WHERE earlier.file_id = font_faces.file_id
    AND earlier.ttc_index = font_faces.ttc_index
    AND earlier.id < font_faces.id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_font_faces_file_ttc
  ON font_faces(file_id, ttc_index);
