import { DOCUMENT } from '@angular/common';
import { Component, Inject } from '@angular/core';

import { NgbPopoverConfig, NgbTooltipConfig } from '@ng-bootstrap/ng-bootstrap';

import { environment } from '~/environments/environment';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  constructor(
    popoverConfig: NgbPopoverConfig,
    tooltipConfig: NgbTooltipConfig,
    @Inject(DOCUMENT) private document: Document
  ) {
    popoverConfig.autoClose = 'outside';
    popoverConfig.container = 'body';
    popoverConfig.placement = 'bottom';

    tooltipConfig.container = 'body';

    if (environment.build === 'ibm') {
      const headEl = this.document.getElementsByTagName('head')[0];
      const newLinkEl = this.document.createElement('link');
      newLinkEl.rel = 'stylesheet';
      newLinkEl.href = 'ibm-overrides.css';

      headEl.appendChild(newLinkEl);
    }
  }
}
