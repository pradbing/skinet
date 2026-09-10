import { Component, inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MatAnchor } from "@angular/material/button";

@Component({
  imports: [MatAnchor],
  selector: 'app-confirmation-dialog',
  styleUrl: './confirmation-dialog.component.scss',
  templateUrl: './confirmation-dialog.component.html',
})
export class ConfirmationDialogComponent {
  dialogRef = inject(MatDialogRef<ConfirmationDialogComponent>);
  data = inject(MAT_DIALOG_DATA);

  onConfirm(){
    this.dialogRef.close(true);
  }

  onCancel(){
    this.dialogRef.close();
  }
}
