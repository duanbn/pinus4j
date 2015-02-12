$(document).ready(function(){

	$('.nav_item').click(function() {
		if ( $(this).hasClass('arrow') ){
			if ( $(this).hasClass('arrow_current') ){
				$(this).toggleClass('arrow_current').next('ul').slideToggle(300);
			}
			else{
				$('.nav_item').removeClass('arrow_current').next('ul').slideUp(300);
				$(this).toggleClass('arrow_current').next('ul').slideToggle(300);
			}
		}
	});	
	
});